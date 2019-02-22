#pragma once

#include <Core/Block.h>
#include <Formats/IRowOutputStream.h>
#include <Formats/FormatSettings.h>
#include <Formats/ProtobufWriter.h>


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
class ProtobufRowOutputStream : public IRowOutputStream
{
public:
    ProtobufRowOutputStream(
        WriteBuffer & buffer_,
        const Block & header_,
        const google::protobuf::Descriptor * message_prototype_);

    void write(const Block & block, size_t row_num) override;
    std::string getContentType() const override { return "application/octet-stream"; }

private:
    ProtobufWriter writer;
    DataTypes data_types;
    std::vector<size_t> column_indices;
};

}
