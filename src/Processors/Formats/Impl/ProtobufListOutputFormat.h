#pragma once

#include "config.h"

#if USE_PROTOBUF
#    include <Processors/Formats/IRowOutputFormat.h>
#   include <Formats/FormatSchemaInfo.h>

namespace DB
{
class ProtobufWriter;
class ProtobufSerializer;

/** Stream designed to serialize data in the google protobuf format.
  * Each row is written as a separated nested message, and all rows are enclosed by a single
  * top-level, envelope message
  *
  * Serializing in the protobuf format requires the 'format_schema' setting to be set, e.g.
  * SELECT * from table FORMAT Protobuf SETTINGS format_schema = 'schema:Message'
  * where schema is the name of "schema.proto" file specifying protobuf schema.
  */
// class ProtobufListOutputFormat final : public IOutputFormat
class ProtobufListOutputFormat final : public IRowOutputFormat
{
public:
    ProtobufListOutputFormat(
        WriteBuffer & out_,
        const Block & header_,
        const ProtobufSchemaInfo & schema_info_,
        bool defaults_for_nullable_google_wrappers_,
        const String & google_protos_path);

    String getName() const override { return "ProtobufListOutputFormat"; }

    String getContentType() const override { return "application/octet-stream"; }

private:
    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override {}

    void finalizeImpl() override;
    void resetFormatterImpl() override;

    std::unique_ptr<ProtobufWriter> writer;
    std::unique_ptr<ProtobufSerializer> serializer;
};

}

#endif
