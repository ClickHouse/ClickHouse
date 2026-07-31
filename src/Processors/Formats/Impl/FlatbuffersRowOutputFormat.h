#pragma once

#include "config.h"

#if USE_FLATBUFFERS

#include <Processors/Formats/IRowOutputFormat.h>
#include <flatbuffers/flexbuffers.h>


namespace DB
{

struct FormatSettings;

/** Serializes the result set as a single schema-less Flatbuffers (FlexBuffers) value.
  *
  * The root value is a vector of rows; every row is a vector of column values in the order
  * of the header. The whole result is accumulated in memory and flushed once at the end,
  * so the format is not streaming and does not support parallel formatting.
  */
class FlatbuffersRowOutputFormat final : public IRowOutputFormat
{
public:
    FlatbuffersRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_);

    String getName() const override { return "FlatbuffersRowOutputFormat"; }

private:
    void writePrefix() override;
    void writeSuffix() override;
    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override {}
    void serializeField(const IColumn & column, const DataTypePtr & data_type, size_t row_num);

    /// flexbuffers::Builder::String reads one byte past the given length (it copies a trailing '\0'
    /// so that C-string readers work), so the source must have an initialized byte at [size].
    /// ColumnString guarantees a trailing '\0', but FixedString and the UUID text buffer do not,
    /// so route every string value through a NUL-terminated scratch buffer.
    void serializeString(std::string_view value);

    /// Serialize a String / FixedString value: as Blob by default (ClickHouse strings are arbitrary
    /// byte sequences), as FlexBuffers String when `output_format_flatbuffers_string_as_string` is set.
    void serializeStringOrBlob(std::string_view value);

    /// Serialize a wide numeric value (Int128/UInt128/Int256/UInt256/Decimal128/Decimal256) as a
    /// little-endian byte sequence, so the produced blob is identical on every architecture (the raw
    /// in-memory bytes would be native-endian and differ on big-endian systems such as s390x).
    template <typename ColumnType>
    void serializeWideNumberAsBlob(const IColumn & column, size_t row_num);

    flexbuffers::Builder builder;
    size_t root_start = 0;
    std::string string_scratch;

    /// ClickHouse String / FixedString values are arbitrary byte sequences (possibly invalid UTF-8,
    /// possibly with embedded zero bytes), while FlexBuffers String is UTF-8 text, so by default
    /// they are serialized as Blob; `output_format_flatbuffers_string_as_string` opts into String.
    bool string_as_string = false;
};

}

#endif
