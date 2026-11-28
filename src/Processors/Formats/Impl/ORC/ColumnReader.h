#pragma once

#include <Processors/Formats/Impl/ORC/RLE.h>
#include <Processors/Formats/Impl/ORC/Vector.h>

#include <Common/UnorderedMapWithMemoryTracking.h>

class DateLUTImpl;

namespace DB::ORC
{

/// The positions from the row indexes of the columns at the beginning of a row group, by column id.
using RowGroupPositions = UnorderedMapWithMemoryTracking<UInt64, PositionProvider>;

/// Access to the streams of one stripe for the column readers.
class StripeStreams
{
public:
    virtual ~StripeStreams() = default;

    [[nodiscard]] virtual const Proto::ColumnEncoding & getEncoding(UInt64 column_id) const = 0;
    /// Returns nullptr if the stripe does not have this stream.
    [[nodiscard]] virtual std::unique_ptr<InputByteStream> getStream(UInt64 column_id, Proto::StreamKind kind) const = 0;
    [[nodiscard]] virtual const VectorWithMemoryTracking<bool> & getSelectedColumns() const = 0;
    /// The time zones for TIMESTAMP (not TIMESTAMP_INSTANT) columns.
    [[nodiscard]] virtual const DateLUTImpl & getWriterTimezone() const = 0;
    [[nodiscard]] virtual const DateLUTImpl & getReaderTimezone() const = 0;
    [[nodiscard]] virtual UInt64 getNumberOfRows() const = 0;
    /// Whether the file has the format version 1.9999 (`UNSTABLE_PRE_2_0` in the reference implementation),
    /// where DECIMAL columns with the precision up to 18 are written as signed RLEv2 integers in the DATA stream
    /// without the SECONDARY stream of scales.
    [[nodiscard]] virtual bool isDecimalAsLong() const = 0;
};

class ColumnReader
{
public:
    ColumnReader(const Type & type, StripeStreams & stripe);
    virtual ~ColumnReader() = default;

    /// Reads the next `num_values` values into `batch`. `incoming_mask` is the mask of the
    /// non-null values of the parent (or nullptr).
    void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask)
    {
        batch.clear();
        append(batch, num_values, incoming_mask, false);
    }

    /// Same, but the string columns with dictionary encoding fill `EncodedStringVectorBatch`
    /// without decoding the dictionary.
    void nextEncoded(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask)
    {
        batch.clear();
        append(batch, num_values, incoming_mask, true);
    }

    /// Skips `num_values` values, returns the number of the skipped non-null values.
    virtual size_t skip(size_t num_values);

    /// Moves to the beginning of a row group, by the positions from the row indexes of the columns.
    virtual void seekToRowGroup(RowGroupPositions & positions);

protected:
    /// Appends the next `num_values` values to `batch`. The values are read in chunks of at most
    /// `max(MIN_CHUNK_SIZE, batch.numElements)` values: the number of values comes from the file
    /// (the lengths of lists and maps can be arbitrary), so the memory for the values must grow only
    /// as the streams prove to contain them.
    void append(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask, bool encoded);

    /// Reads `num_values` values to the positions from `offset` in `batch` (`batch.numElements` is
    /// already `offset + num_values`, and its null mask is filled). With `offset` zero, the batch is
    /// filled anew, and the batches of the children must be cleared. `not_null` is the null mask of
    /// these values (or nullptr).
    virtual void appendValues(ColumnVectorBatch & batch, size_t offset, size_t num_values, const char * not_null, bool encoded) = 0;

    /// For the readers of the compound types.
    static void appendChild(ColumnReader & child, ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask, bool encoded)
    {
        child.append(batch, num_values, incoming_mask, encoded);
    }

    static constexpr size_t MIN_CHUNK_SIZE = 1024 * 1024;

    const UInt64 column_id;
    std::unique_ptr<BooleanRleDecoder> not_null_decoder;
};

std::unique_ptr<ColumnReader> buildColumnReader(const Type & type, StripeStreams & stripe);

}
