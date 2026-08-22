#pragma once

#include <Core/MySQL/IMySQLWritePacket.h>

namespace DB
{
namespace MySQLProtocol
{
namespace PreparedStatements
{
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response_ok
class PreparedStatementResponseOK : public IMySQLWritePacket
{
public:
    const uint8_t status = 0x00;
    uint32_t statement_id;
    uint16_t num_columns;
    uint16_t num_params;
    const uint8_t reserved_1 = 0;
    uint16_t warnings_count;

protected:
    size_t getPayloadSize() const override;

    void writePayloadImpl(WriteBuffer & buffer) const override;

public:
    PreparedStatementResponseOK(uint32_t statement_id_, uint16_t num_columns_, uint16_t num_params_, uint16_t warnings_count_);
};
}
}
}
