#pragma once

#include <IO/ReadBuffer.h>

namespace DB
{

/// Throws INCORRECT_DATA with the same "Size of JSON object ... is extremely large" message the
/// parallel-parsing segmentation engine uses, so every JSONEachRow-family read path reports an
/// oversized row the same way. position is the byte offset in the input, max_bytes is the cap,
/// current_bytes is the size of the offending row.
[[noreturn]] void throwJSONEachRowObjectTooLarge(size_t position, size_t max_bytes, size_t current_bytes);

/// A zero-copy pass-through ReadBuffer that bounds the number of bytes a single
/// JSONEachRow row may consume. It shares the working buffer of the nested buffer
/// (no extra allocation or copying), but never exposes more than max_bytes_per_row
/// of it at once, so that consuming an oversized row always hits a refill where the
/// limit is checked. When one row exceeds the limit it throws INCORRECT_DATA, matching
/// the cap the parallel-parsing segmentation engine already enforces, so the
/// non-parallel read path cannot OOM on a single huge JSON value (e.g. a whole file
/// that is one giant JSON object, mmapped into a single buffer).
///
/// One instance wraps the parsing of a single row: the per-row byte counter starts at
/// zero on construction. On destruction it syncs the nested buffer's position to where
/// the row finished, so the caller can keep reading from the nested buffer.
class JSONEachRowRowSizeLimitReadBuffer : public ReadBuffer
{
public:
    JSONEachRowRowSizeLimitReadBuffer(ReadBuffer & in_, size_t max_bytes_per_row_);
    ~JSONEachRowRowSizeLimitReadBuffer() override;

private:
    bool nextImpl() override;

    ReadBuffer & in;
    const size_t max_bytes_per_row;
    /// Bytes of this row already consumed from the nested buffer across previous refills.
    size_t counted_bytes = 0;
};

}
