#pragma once

#include <Common/config.h>
#if USE_PROTOBUF

#include <DataTypes/IDataType.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/ProtobufReader.h>

namespace DB
{
class Block;
class FormatSchemaInfo;


/** Interface of stream, that allows to read data by rows.
  */
class ProtobufRowInputFormat : public IRowInputFormat
{
public:
    ProtobufRowInputFormat(ReadBuffer & in_, const Block & header, Params params, const FormatSchemaInfo & info);
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
