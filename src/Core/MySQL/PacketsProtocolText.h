#pragma once

#include <Core/MySQL/IMySQLWritePacket.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

namespace MySQLProtocol
{

namespace ProtocolText
{

class ResultSetRow : public IMySQLWritePacket
{
protected:
    const Columns & columns;
    int row_num;
    size_t payload_size = 0;
    std::vector<String> serialized;

    size_t getPayloadSize() const override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    ResultSetRow(const DataTypes & data_types, const Columns & columns_, int row_num_);
};

}

}

}
