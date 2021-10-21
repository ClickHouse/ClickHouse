#pragma once

#include "config_formats.h"
#if USE_CAPNP

#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/FormatSchemaInfo.h>
#include <Formats/CapnProtoUtils.h>
#include <capnp/schema.h>
#include <capnp/dynamic.h>
#include <kj/io.h>

namespace DB
{
class CapnProtoOutputStream : public kj::OutputStream
{
public:
    CapnProtoOutputStream(WriteBuffer & out_);

    void write(const void * buffer, size_t size) override;

private:
    WriteBuffer & out;
};

class CapnProtoRowOutputFormat : public IRowOutputFormat
{
public:
    CapnProtoRowOutputFormat(
        WriteBuffer & out_,
        const Block & header_,
        const RowOutputFormatParams & params_,
        const FormatSchemaInfo & info,
        const FormatSettings & format_settings_);

    String getName() const override { return "CapnProtoRowOutputFormat"; }

    void write(const Columns & columns, size_t row_num) override;

    void writeField(const IColumn &, const ISerialization &, size_t) override { }

private:
    Names column_names;
    DataTypes column_types;
    capnp::StructSchema schema;
    std::unique_ptr<CapnProtoOutputStream> output_stream;
    const FormatSettings format_settings;
    CapnProtoSchemaParser schema_parser;
};

}

#endif // USE_CAPNP
