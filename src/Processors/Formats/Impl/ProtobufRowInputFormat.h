#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_formats.h"
#endif

#if USE_PROTOBUF
#    include <DataTypes/IDataType.h>
#    include <Formats/ProtobufReader.h>
#    include <Processors/Formats/IRowInputFormat.h>

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
class ProtobufRowInputFormat : public IRowInputFormat
{
public:
    ProtobufRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSchemaInfo & info_);
    ~ProtobufRowInputFormat() override;

    String getName() const override { return "ProtobufRowInputFormat"; }

    bool readRow(MutableColumns & columns, RowReadExtension & extra) override;
    bool allowSyncAfterError() const override;
    void syncAfterError() override;

private:
    DataTypes data_types;
    ProtobufReader reader;
};

}
#endif
