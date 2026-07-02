#pragma once

#include "config.h"

#if USE_FLATBUFFERS

#include <Processors/Formats/IRowOutputFormat.h>
#include <flatbuffers/flexbuffers.h>


namespace DB
{

class FormatSettings;

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

    flexbuffers::Builder builder;
    size_t root_start = 0;
};

}

#endif
