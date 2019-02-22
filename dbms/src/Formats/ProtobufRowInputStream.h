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


/** Interface of stream, that allows to read data by rows.
  */
class ProtobufRowInputStream : public IRowInputStream
{
public:
    ProtobufRowInputStream(ReadBuffer & in_, const Block & header, const FormatSchemaInfo & info);
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
