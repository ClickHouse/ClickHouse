#pragma once

#include "config.h"

#if USE_AVRO_RUST

#include <Core/Block.h>
#include <Core/Names.h>
#include <Formats/FormatSettings.h>
#include <base/types.h>

#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace DB::AvroRust
{

class Schema;
class Node;

/// A metadata reader can reject a schema kind on visit, before its ClickHouse representation erases that distinction.
/// Invoked only while compiling readers; an error becomes a throwing reader, not a per-value callback.
using ValueValidator = std::function<std::optional<String>(std::string_view column, Node node, size_t container_depth)>;

/// Compiles the writer schema and ClickHouse column mapping once. Output buffers
/// belong to C++; the Rust executor only writes within the supplied capacities.
class Plan
{
public:
    Plan(
        const Block & header,
        const Schema & schema,
        bool allow_missing_fields,
        bool null_as_default,
        const FormatSettings & settings,
        const NameSet & absent_columns = {},
        ValueValidator value_validator = {});
    ~Plan();

    Plan(const Plan &) = delete;
    Plan & operator=(const Plan &) = delete;
    Plan(Plan &&) = delete;
    Plan & operator=(Plan &&) = delete;

    /// Resets block-scoped work accounting (the budget of items that occupy no bytes). Call once before the first
    /// decode of every OCF block; every window and batch of that block shares the budget, and a row that a window
    /// cut does not reset it.
    void beginBlock();

    /// Every call starts at a row boundary and decodes exactly `max_rows`.
    /// Calls after one `beginBlock` are consecutive batches of that block.
    /// On failure, only fully committed rows remain in the destination columns.
    /// `missing[column][row]` is relative to this invocation, not the column size. It is set for a value that the
    /// file does not have: a column absent from the schema, the Avro null of a union that the type cannot hold
    /// (`input_format_null_as_default`), a union branch that the row did not take, an array of records without items.
    /// `missing[column]` is empty, not all zeros, for a column that the plan always reads from the file, so
    /// no per-row work is spent on it: a caller reads a flag only for rows below `missing[column].size()`.
    UInt64 decode(
        std::span<const UInt8> input,
        UInt64 max_rows,
        MutableColumns & columns,
        std::vector<std::vector<UInt8>> & missing);

    /// The payload of a block, or what is left of it, that the decoder is given at a time.
    struct InputWindow
    {
        /// The bytes that are available: the ones the decoder has not consumed yet, then the ones that arrived after them.
        std::span<const UInt8> bytes;
        /// True while more bytes of the payload may follow `bytes`; false: the payload ends with them.
        bool more = false;
    };

    /// Called when the decoder is done with the first `consumed` bytes of the window it was given last and needs
    /// more. It returns the window that starts with the first byte that was not consumed and goes as far as the bytes
    /// that have arrived now (at least one more than before, unless `more` is false in the result): the caller drops
    /// the consumed bytes and appends what follows. An exception thrown from it fails the decode like any other error.
    using InputSource = std::function<InputWindow(UInt64 consumed)>;

    /// Decodes exactly `max_rows` rows of a block whose payload arrives window by window, starting with `window`.
    /// Returns the bytes of the last window that the rows occupy (the caller keeps everything from there), after
    /// the decoder has asked `next` for the windows that it needed.
    ///
    /// The decoder asks when the window ends inside a row, and it has consumed everything that it was done with: a
    /// `bytes`, `string` or `fixed` value that the plan does not read is skipped as it arrives and is never held, so the
    /// memory of the caller is the size of its window (and of the largest value that is read into a column), not of a
    /// row, whatever the size of the values of it that are skipped. Values that are read are decoded from the window
    /// when it holds them whole. The budget of work of the block (`beginBlock`) is shared by all the windows.
    ///
    /// When `more` is false in the last window, rows that it cannot complete are an exception, as in `decode`. All other
    /// errors are exceptions, too, and leave the rows that were committed before them in `columns`.
    ///
    /// Unlike `decode`, `missing` is appended to: `missing[column][row]` is relative to the size that it had when
    /// the call started. A column that is never missing stays empty, as in `decode`.
    UInt64 decodeStream(
        InputWindow window,
        const InputSource & next,
        UInt64 max_rows,
        MutableColumns & columns,
        std::vector<std::vector<UInt8>> & missing);

    /// Outcome of `decodeDatum`.
    struct DatumResult
    {
        /// False: the input ended inside the datum, so nothing was committed and more bytes may complete it.
        bool complete = false;
        /// Bytes of `input` the datum occupies; 0 unless `complete`.
        UInt64 consumed = 0;
    };

    /// Decodes exactly one datum (one row) that may continue in bytes that have not arrived yet.
    /// The executor has no mid-row state, so a datum that did not fit `input` is decoded again from its start.
    ///
    /// With `more_input`, input that ends inside the datum is reported as `complete == false` (decided by the
    /// structured status of the decoder: end of input, or a block count that cannot fit the remaining bytes), and
    /// the destination columns are left untouched. Without `more_input` the same condition is a decode error and
    /// is thrown like in `decode`. All other errors are thrown in both cases.
    DatumResult decodeDatum(
        std::span<const UInt8> input,
        bool more_input,
        MutableColumns & columns,
        std::vector<std::vector<UInt8>> & missing);

    UInt64 committedRows() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

}

#endif
