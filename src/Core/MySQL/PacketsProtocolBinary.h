#pragma once

#include <vector>
#include <Columns/IColumn.h>
#include <Core/DecimalFunctions.h>
#include <Core/MySQL/IMySQLReadPacket.h>
#include <Core/MySQL/IMySQLWritePacket.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{
namespace MySQLProtocol
{
namespace ProtocolBinary
{
class ResultSetRow : public IMySQLWritePacket
{
protected:
    size_t row_num;
    const Columns & columns;
    const DataTypes & data_types;
    const Serializations & serializations;

    std::vector<String> serialized = std::vector<String>(columns.size());

    // See https://dev.mysql.com/doc/dev/mysql-server/8.1.0/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row
    size_t null_bitmap_size = (columns.size() + 7 + 2) / 8;
    std::vector<char> null_bitmap = std::vector<char>(null_bitmap_size, static_cast<char>(0));

    size_t payload_size = 0;

    size_t getPayloadSize() const override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    ResultSetRow(const Serializations & serializations_, const DataTypes & data_types_, const Columns & columns_, size_t row_num_);
};
}
}
}
