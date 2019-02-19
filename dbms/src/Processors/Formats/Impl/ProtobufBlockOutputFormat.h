#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/ProtobufWriter.h>
#include <Processors/Formats/IOutputFormat.h>


namespace google
{
namespace protobuf
{
    class Message;
}
}


namespace DB
{
/** Stream designed to serialize data in the google protobuf format.
  * Each row is written as a separated message.
  * These messages are delimited according to documentation
  * https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/util/delimited_message_util.h
  * Serializing in the protobuf format requires the 'format_schema' setting to be set, e.g.
  * SELECT * from table FORMAT Protobuf SETTINGS format_schema = 'schema:Message'
  * where schema is the name of "schema.proto" file specifying protobuf schema.
  */
class ProtobufBlockOutputFormat : public IOutputFormat
{
public:
    ProtobufBlockOutputFormat(
        WriteBuffer & out,
        Block header,
        const google::protobuf::Descriptor * message_prototype,
        const FormatSettings & format_settings);

    String getName() const override { return "ProtobufBlockOutputFormat"; }

    void consume(Chunk chunk) override;
    std::string getContentType() const override { return "application/octet-stream"; }

private:
    ProtobufWriter writer;
    const Block header;
    const FormatSettings format_settings;
};

}
