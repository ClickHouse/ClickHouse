#pragma once

#include "config.h"

#if USE_ORC
#include <Common/PODArray_fwd.h>
#include <Common/VectorWithMemoryTracking.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFilterInfo.h>
#include <Processors/Formats/Impl/ORC/Writer.h>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = VectorWithMemoryTracking<DataTypePtr>;
class WriteBuffer;


class ORCOutputStream : public ORC::OutputStream
{
public:
    explicit ORCOutputStream(WriteBuffer & out_);

    void write(const void * buf, size_t length) override;

private:
    WriteBuffer & out;
};


class ORCBlockOutputFormat final : public IOutputFormat
{
public:
    ORCBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_, ColumnMapperPtr column_mapper_ = nullptr);

    String getName() const override { return "ORCBlockOutputFormat"; }

private:
    void consume(Chunk chunk) override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    /// For Iceberg writes, attaches the field-id `iceberg.id` type attribute so readers project
    /// by id. `column_path` is the dotted name (`t.x`, `arr.element`) used to look the id up.
    std::unique_ptr<ORC::Type> getORCType(const DataTypePtr & type, const String & column_path);

    void prepareWriter();

    const FormatSettings format_settings;
    ORCOutputStream output_stream;
    DataTypes data_types;
    std::unique_ptr<ORC::Writer> writer;
    std::unique_ptr<ORC::Type> schema;
    ORC::WriterOptions options;
    /// Non-null only for Iceberg writes; maps dotted column name -> Iceberg field-id.
    ColumnMapperPtr column_mapper;
};

}
#endif
