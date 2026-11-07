#pragma once

#include "config.h"

#if USE_AVRO_RUST

#include <Columns/IColumn_fwd.h>
#include <Common/PODArray.h>
#include <Core/Block_fwd.h>
#include <Processors/Formats/Impl/Avro/AvroRustSchema.h>
#include <base/types.h>

#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace DB
{

class ReadBuffer;

namespace AvroRust
{

class Plan;

namespace detail
{
class RawPayload;
class PlainStream;
}

/// Avro Object Container File reader. Framing (header, block headers, sync markers) is parsed by the
/// Rust library on a window of bytes; decompression is done here with the in-tree, memory-tracked codecs.
///
/// All errors are `AVRO_EXCEPTION` with the message prefix `avro::Exception: `.
///
/// Memory is bounded by the rows, not by the block. The payload of a block is never held whole (except for the
/// `snappy` codec, below): the decoder is given it in windows. For the null codec a window is the piece of the input
/// as it is (a piece of at least 64 KiB; smaller ones are gathered into a window of `PLAIN_WINDOW_BYTES`), with
/// nothing copied but the bytes of a value that the end of a piece cuts; for `deflate`, `zstandard`,
/// `bzip2` and `xz` it is a window of plain bytes of `PLAIN_WINDOW_BYTES`, filled by the incremental decoders of the
/// codec (the compressed input is taken from the buffer of `in` in the pieces it has, without a copy). The rows of a
/// batch are decoded from the window and the bytes that the decoder is done with are dropped as it goes, also in the
/// middle of a row: a `bytes`, `string` or `fixed` value that the query does not read is skipped as it arrives and is
/// never held, whatever its size. A value that is read into a column makes the window grow, by doubling and only until
/// the value fits, so the window is at most twice the size of the largest value that is read (plus `PLAIN_WINDOW_BYTES`
/// or the piece of the input), whatever the size of the row or of the block. Nothing is allocated from a size that the
/// file declares: buffers grow only with the bytes that were really read or decompressed.
///
/// `snappy` is the exception: its format has no streaming interface and the checksum of a block covers the whole plain
/// payload, so the compressed block and its plain payload are held whole, as in the library this reader replaces
/// (`contrib/avro`), which also decompresses a snappy block at once. In exchange, `canCountRows` can count a block's
/// rows without decoding them.
class OcfReader
{
public:
    /// Plain bytes that are made available to the decoder at a time, before a row needs more.
    static constexpr size_t PLAIN_WINDOW_BYTES = 1ULL << 20;

    /// Reads and validates the file header and parses the schema.
    explicit OcfReader(ReadBuffer & in_);
    ~OcfReader();

    OcfReader(const OcfReader &) = delete;
    OcfReader & operator=(const OcfReader &) = delete;

    /// True only for the snappy codec with a schema whose rows take at least one byte: the block's checksum
    /// and the object count check against the plain size then make `rows` trustworthy without decoding.
    bool canCountRows() const { return codec == AVRO_CODEC_SNAPPY && min_record_size > 0; }

    const Schema & schema() const { return *schema_; }

    /// Finishes the previous block (the rest of its payload is read through, which also validates a compressed
    /// stream up to its end, and its sync marker is checked), then moves to the next block.
    /// Returns false at the clean end of the file.
    bool nextBlock();

    /// Object count of the current block.
    UInt64 rows() const { return block_rows; }

    /// Rows of the current block that were neither decoded nor discarded yet.
    UInt64 rowsLeft() const { return rows_left; }

    /// Decodes the next `max_rows` (at most `rowsLeft`) rows of the current block through `plan` and appends them
    /// to `columns`, and their `missing` flags (relative to the call, `missing[column][row]`, empty for a column
    /// that is never missing) to the cleared `missing`.
    /// The payload is taken window by window as the rows need it, so the memory used is independent of the size of
    /// the block and of the skipped values; the cancellation of the query is checked between windows. The first call
    /// for a block begins the block in `plan` (`Plan::beginBlock`): the budget of its work is shared by all the
    /// windows of the block.
    /// On failure only fully decoded rows remain in `columns`.
    void decodeRows(Plan & plan, UInt64 max_rows, MutableColumns & columns, std::vector<std::vector<UInt8>> & missing);

    /// The rows that `decodeRows` has put into the columns in the call that is running or failed last, to number the
    /// row that an exception was thrown at: those that the failed call committed before it.
    UInt64 committedRows(const Plan & plan) const;

    /// Counts `n` rows of the current block as done without decoding them. Only for `canCountRows`.
    void discardRows(UInt64 n);

    const std::map<std::string, std::vector<uint8_t>> & metadata() const { return metadata_; }

private:
    friend class detail::RawPayload;

    /// Appends bytes from `in` to `window` until it holds `target` unconsumed bytes or the input ended.
    void fillWindow(size_t target);
    /// Appends the bytes that are already in the buffer of `in`, without waiting for input, until the window holds
    /// `limit` unconsumed bytes.
    void fillWindowFromBuffer(size_t limit);
    /// Drops the first `n` unconsumed bytes of the window in constant time.
    void consumeWindow(size_t n);
    void compactWindow();
    void clearWindow();
    const UInt8 * windowData() const { return window.data() + window_begin; }
    size_t windowSize() const { return window.size() - window_begin; }
    void checkSyncOfCurrentBlock();

    /// Reads through the rest of the current block, so that the sync marker that follows is next.
    void finishBlock();
    /// Takes the whole payload of a snappy block and decompresses it into `plain`.
    void loadSnappyBlock();

    /// Appends up to `limit` plain bytes to `plain`, and checks the object count against the size of the whole payload
    /// when it has ended. Not for the null codec, which has no plain copy of the payload.
    void producePlain(size_t limit);
    /// Makes room in `plain` by dropping the consumed bytes, then appends the next window. False if nothing was added.
    bool fillPlain();
    /// `decodeRows` of every codec but null: from the window of plain bytes in `plain`.
    void decodeFromPlain(Plan & plan, UInt64 max_rows, MutableColumns & columns, std::vector<std::vector<UInt8>> & missing);
    /// `decodeRows` of the null codec: from the input itself. Nothing is copied but the bytes of a value that the end of
    /// a piece of the input cuts (with the start of the next piece), and the pieces that are too small to be worth a
    /// call of the decoder, which are gathered into a window of `PLAIN_WINDOW_BYTES`.
    void decodeFromInput(Plan & plan, UInt64 max_rows, MutableColumns & columns, std::vector<std::vector<UInt8>> & missing);
    const UInt8 * plainData() const { return plain.data() + plain_begin; }
    size_t plainSize() const { return plain.size() - plain_begin; }

    ReadBuffer & in;
    std::map<std::string, std::vector<uint8_t>> metadata_;
    UInt32 codec = 0;
    UInt8 sync[16] = {};
    UInt64 min_record_size = 0;

    /// Bytes taken from `in` that Rust still has to look at (headers, sync markers), from `window_begin` on.
    /// The file header is parsed on a window that grows geometrically; whatever was read past it stays here and is
    /// consumed as the start of the following blocks, and the payload of the first block starts with it.
    PODArray<UInt8> window;
    size_t window_begin = 0;

    bool have_block = false;
    UInt64 block_rows = 0;
    UInt64 rows_left = 0;
    /// Bytes of the (compressed) payload of the block that are neither handed to the decoder nor skipped.
    UInt64 raw_left = 0;

    /// The plain bytes of the block that were produced and not consumed, from `plain_begin` on. For the null codec,
    /// only what `decodeFromInput` copies from the input: the bytes of a value that the end of a piece of the input cuts,
    /// with the start of the pieces that follow, and, between two calls, the part of that copy that the rows did not use.
    PODArray<UInt8> plain;
    size_t plain_begin = 0;
    /// The plain bytes produced for the block so far, consumed or not.
    UInt64 plain_total = 0;
    /// Decodes the payload incrementally; none for the null codec (the decoder reads the input itself) and for snappy
    /// (decompressed at once).
    std::unique_ptr<detail::PlainStream> stream;
    /// True for a block that is entirely in `plain`.
    bool plain_complete = false;
    /// The whole compressed block of the snappy codec.
    PODArray<UInt8> snappy_storage;

    /// `decodeRows` of the current block has begun the block in the plan.
    bool plan_begun = false;
    /// Rows of the last `decodeRows` that finished without an exception; and whether `decodeRows` has not returned.
    UInt64 batch_committed = 0;
    bool in_decode = false;

    std::optional<Schema> schema_;
};

/// Receives one batch of freshly decoded rows: the columns of the header, `missing[column][row]` set for a value that
/// the file does not have (the Avro null of a union), and the number of rows. `missing[column]` is empty for a
/// column that the plan always reads (see `Plan::decode`).
using DecodedBatchCallback = std::function<void(MutableColumns & columns, const std::vector<std::vector<UInt8>> & missing, UInt64 rows)>;

/// Decodes the remaining blocks of `reader` through `plan`, whose header is `header`, in batches of at most
/// `CANCELLATION_CHECK_PERIOD_ROWS` rows, so a file is never held whole and a declared block size neither reserves
/// memory up front nor defers cancellation until the whole block is materialized. Every batch is decoded into new empty
/// columns and handed to `callback`.
void forEachDecodedBatch(OcfReader & reader, Plan & plan, const Block & header, const DecodedBatchCallback & callback);

}

}

#endif
