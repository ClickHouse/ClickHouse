#pragma once

#include "config_formats.h"

#if USE_PROTOBUF
#    include <Formats/FormatSchemaInfo.h>
#    include <Processors/Formats/IRowInputFormat.h>
#    include <Processors/Formats/ISchemaReader.h>

namespace DB
{
class Block;
class ProtobufReader;
class ProtobufSerializer;
class ReadBuffer;

/** Stream designed to deserialize data from the google protobuf format.
  * One nested Protobuf message is parsed as one row of data.
  *
  * Parsing of the protobuf format requires the 'format_schema' setting to be set, e.g.
  * INSERT INTO table FORMAT Protobuf SETTINGS format_schema = 'schema:Message'
  * where schema is the name of "schema.proto" file specifying protobuf schema.
  */
class ProtobufListInputFormat final : public IRowInputFormat
{
public:
    ProtobufListInputFormat(
        ReadBuffer & in_,
        const Block & header_,
        const Params & params_,
        const FormatSchemaInfo & schema_info_,
        bool flatten_google_wrappers_);

    String getName() const override { return "ProtobufListInputFormat"; }

private:
    bool readRow(MutableColumns & columns, RowReadExtension & row_read_extension) override;

    std::unique_ptr<ProtobufReader> reader;
    std::vector<size_t> missing_column_indices;
    std::unique_ptr<ProtobufSerializer> serializer;
};

class ProtobufListSchemaReader : public IExternalSchemaReader
{
public:
    explicit ProtobufListSchemaReader(const FormatSettings & format_settings);

    NamesAndTypesList readSchema() override;

private:
    const FormatSchemaInfo schema_info;
};

}

#endif
