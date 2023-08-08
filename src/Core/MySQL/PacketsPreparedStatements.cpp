#include <Core/MySQL/PacketsPreparedStatements.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace MySQLProtocol
{
    namespace PreparedStatements
    {
        size_t PrepareStatementResponseOK::getPayloadSize() const
        {
            return 13;
        }

        void PrepareStatementResponseOK::writePayloadImpl(WriteBuffer & buffer) const
        {
            buffer.write(reinterpret_cast<const char *>(&status), 1);
            buffer.write(reinterpret_cast<const char *>(&statement_id), 4);
            buffer.write(reinterpret_cast<const char *>(&num_columns), 2);
            buffer.write(reinterpret_cast<const char *>(&num_params), 2);
            buffer.write(reinterpret_cast<const char *>(&reserved_1), 1);
            buffer.write(reinterpret_cast<const char *>(&warnings_count), 2);
            buffer.write(0x0); // RESULTSET_METADATA_NONE
        }

        void PrepareStatementResponseOK::readPayloadImpl([[maybe_unused]] ReadBuffer & payload)
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PrepareStatementResponseOK::readPayloadImpl is not implemented");
        }

        PrepareStatementResponseOK::PrepareStatementResponseOK(
            uint32_t statement_id_, uint16_t num_columns_, uint16_t num_params_, uint16_t warnings_count_)
            : statement_id(statement_id_), num_columns(num_columns_), num_params(num_params_), warnings_count(warnings_count_)
        {
        }
    }
}
}
