#include <Core/MySQL/PacketsPreparedStatements.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace MySQLProtocol
{
namespace PreparedStatements
{
size_t PreparedStatementResponseOK::getPayloadSize() const
{
    // total = 13
    return 1 // status
        + 4 // statement_id
        + 2 // num_columns
        + 2 // num_params
        + 1 // reserved_1 (filler)
        + 2 // warnings_count
        + 1; // metadata_follows
}

void PreparedStatementResponseOK::writePayloadImpl(WriteBuffer & buffer) const
{
    buffer.write(reinterpret_cast<const char *>(&status), 1);
    buffer.write(reinterpret_cast<const char *>(&statement_id), 4);
    buffer.write(reinterpret_cast<const char *>(&num_columns), 2);
    buffer.write(reinterpret_cast<const char *>(&num_params), 2);
    buffer.write(reinterpret_cast<const char *>(&reserved_1), 1);
    buffer.write(reinterpret_cast<const char *>(&warnings_count), 2);
    buffer.write(0x0); // RESULTSET_METADATA_NONE
}

PreparedStatementResponseOK::PreparedStatementResponseOK(
    uint32_t statement_id_, uint16_t num_columns_, uint16_t num_params_, uint16_t warnings_count_)
    : statement_id(statement_id_), num_columns(num_columns_), num_params(num_params_), warnings_count(warnings_count_)
{
}
}
}
}
