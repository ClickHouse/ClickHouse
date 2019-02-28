#pragma once

#include <Common/config.h>
#if USE_PROTOBUF

#include <DataTypes/IDataType.h>
#include <Formats/IRowInputStream.h>
#include <Formats/ProtobufReader.h>

namespace DB
{
class Block;
class FormatSchemaInfo;


/** Stream designed to deserialize data from the google protobuf format.
  * Each row is read as a separated message.
  * These messages are delimited according to documentation
  * https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/util/delimited_message_util.h
  * Serializing in the protobuf format requires the 'format_schema' setting to be set, e.g.
  * INSERT INTO table FORMAT Protobuf SETTINGS format_schema = 'schema:Message'
  * where schema is the name of "schema.proto" file specifying protobuf schema.
  */
class ProtobufRowInputStream : public IRowInputStream
{
public:
    ProtobufRowInputStream(ReadBuffer & in_, const Block & header, const FormatSchemaInfo & format_schema);
    ~ProtobufRowInputStream() override;

    bool read(MutableColumns & columns, RowReadExtension & extra) override;
    bool allowSyncAfterError() const override;
    void syncAfterError() override;

private:
    DataTypes data_types;
    ProtobufReader reader;
};

}
#endif
