#pragma once

#include <Processors/Formats/Impl/ORC/RLE.h>
#include <Processors/Formats/Impl/ORC/Vector.h>

#include <unordered_map>

class DateLUTImpl;

namespace DB::ORC
{

/// Access to the streams of one stripe for the column readers.
class StripeStreams
{
public:
    virtual ~StripeStreams() = default;

    virtual const Proto::ColumnEncoding & getEncoding(UInt64 column_id) const = 0;
    /// Returns nullptr if the stripe does not have this stream.
    virtual std::unique_ptr<InputByteStream> getStream(UInt64 column_id, Proto::StreamKind kind) const = 0;
    virtual const std::vector<bool> & getSelectedColumns() const = 0;
    /// The time zones for TIMESTAMP (not TIMESTAMP_INSTANT) columns.
    virtual const DateLUTImpl & getWriterTimezone() const = 0;
    virtual const DateLUTImpl & getReaderTimezone() const = 0;
    virtual UInt64 getNumberOfRows() const = 0;
};

class ColumnReader
{
public:
    ColumnReader(const Type & type, StripeStreams & stripe);
    virtual ~ColumnReader() = default;

    /// Reads the next `num_values` values into `batch`. `incoming_mask` is the mask of the
    /// non-null values of the parent (or nullptr).
    virtual void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask);

    /// Same, but the string columns with dictionary encoding fill `EncodedStringVectorBatch`
    /// without decoding the dictionary.
    virtual void nextEncoded(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask)
    {
        next(batch, num_values, incoming_mask);
    }

    /// Skips `num_values` values, returns the number of the skipped non-null values.
    virtual size_t skip(size_t num_values);

    /// Moves to the beginning of a row group, by the positions from the row indexes of the columns.
    virtual void seekToRowGroup(std::unordered_map<UInt64, PositionProvider> & positions);

protected:
    const UInt64 column_id;
    std::unique_ptr<BooleanRleDecoder> not_null_decoder;
};

std::unique_ptr<ColumnReader> buildColumnReader(const Type & type, StripeStreams & stripe);

}
