#pragma once

#include "config.h"
#if USE_CAPNP

#    include <Formats/CapnProtoSchema.h>
#    include <Formats/CapnProtoSerializer.h>
#    include <Formats/FormatSchemaInfo.h>
#    include <Processors/Formats/IRowOutputFormat.h>
#    include <capnp/dynamic.h>
#    include <capnp/schema.h>
#    include <kj/io.h>

namespace DB
{

class CapnProtoOutputStream : public kj::OutputStream
{
public:
    explicit CapnProtoOutputStream(WriteBuffer & out_);

    void write(const void * buffer, size_t size) override;

private:
    WriteBuffer & out;
};

class CapnProtoRowOutputFormat final : public IRowOutputFormat
{
public:
    CapnProtoRowOutputFormat(
        WriteBuffer & out_,
        const Block & header_,
        const FormatSchemaInfo & info,
        const FormatSettings & format_settings_);

    String getName() const override { return "CapnProtoRowOutputFormat"; }

private:
    void write(const Columns & columns, size_t row_num) override;

    void writeField(const IColumn &, const ISerialization &, size_t) override { }

    Names column_names;
    DataTypes column_types;
    capnp::StructSchema schema;
    std::unique_ptr<CapnProtoOutputStream> output_stream;
    CapnProtoSchemaParser schema_parser;
    std::unique_ptr<CapnProtoSerializer> serializer;

};

}

#endif // USE_CAPNP
