#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_formats.h"
#endif

#if USE_PROTOBUF
#    include <Processors/Formats/IRowInputFormat.h>

namespace DB
{
class Block;
class FormatSchemaInfo;
class ProtobufReader;
class ProtobufSerializer;


/** Stream designed to deserialize data from the google protobuf format.
  * One Protobuf message is parsed as one row of data.
  *
  * Input buffer may contain single protobuf message (use_length_delimiters_ = false),
  * or any number of messages (use_length_delimiters = true). In the second case
  * parser assumes messages are length-delimited according to documentation
  * https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/util/delimited_message_util.h
  * Parsing of the protobuf format requires the 'format_schema' setting to be set, e.g.
  * INSERT INTO table FORMAT Protobuf SETTINGS format_schema = 'schema:Message'
  * where schema is the name of "schema.proto" file specifying protobuf schema.
  */
class ProtobufRowInputFormat : public IRowInputFormat
{
public:
    ProtobufRowInputFormat(ReadBuffer & in_, const Block & header_, const Params & params_, const FormatSchemaInfo & schema_info_, bool with_length_delimiter_);
    ~ProtobufRowInputFormat() override;

    String getName() const override { return "ProtobufRowInputFormat"; }

    bool readRow(MutableColumns & columns, RowReadExtension &) override;
    bool allowSyncAfterError() const override;
    void syncAfterError() override;

private:
    std::unique_ptr<ProtobufReader> reader;
    std::vector<size_t> missing_column_indices;
    std::unique_ptr<ProtobufSerializer> serializer;
};

}
#endif
