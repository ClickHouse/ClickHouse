#include <Interpreters/TablesStatus.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/ProtocolDefines.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int LOGICAL_ERROR;
}

void TableStatus::write(WriteBuffer & out) const
{
    writeBinary(is_replicated, out);
    if (is_replicated)
    {
        writeVarUInt(absolute_delay, out);
    }
}

void TableStatus::read(ReadBuffer & in)
{
    absolute_delay = 0;
    readBinary(is_replicated, in);
    if (is_replicated)
    {
        readVarUInt(absolute_delay, in);
    }
}

void TablesStatusRequest::write(WriteBuffer & out, UInt64 server_protocol_revision) const
{
    if (server_protocol_revision < DBMS_MIN_REVISION_WITH_TABLES_STATUS)
        throw Exception(
                "Logical error: method TablesStatusRequest::write is called for unsupported server revision",
                ErrorCodes::LOGICAL_ERROR);

    writeVarUInt(tables.size(), out);
    for (const auto & table_name : tables)
    {
        writeBinary(table_name.database, out);
        writeBinary(table_name.table, out);
    }
}

void TablesStatusRequest::read(ReadBuffer & in, UInt64 client_protocol_revision)
{
    if (client_protocol_revision < DBMS_MIN_REVISION_WITH_TABLES_STATUS)
        throw Exception(
                "method TablesStatusRequest::read is called for unsupported client revision",
                ErrorCodes::LOGICAL_ERROR);

    size_t size = 0;
    readVarUInt(size, in);

    if (size > DEFAULT_MAX_STRING_SIZE)
        throw Exception("Too large collection size.", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

    for (size_t i = 0; i < size; ++i)
    {
        QualifiedTableName table_name;
        readBinary(table_name.database, in);
        readBinary(table_name.table, in);
        tables.emplace(std::move(table_name));
    }
}

void TablesStatusResponse::write(WriteBuffer & out, UInt64 client_protocol_revision) const
{
    if (client_protocol_revision < DBMS_MIN_REVISION_WITH_TABLES_STATUS)
        throw Exception(
                "method TablesStatusResponse::write is called for unsupported client revision",
                ErrorCodes::LOGICAL_ERROR);

    writeVarUInt(table_states_by_id.size(), out);
    for (const auto & kv: table_states_by_id)
    {
        const QualifiedTableName & table_name = kv.first;
        writeBinary(table_name.database, out);
        writeBinary(table_name.table, out);

        const TableStatus & status = kv.second;
        status.write(out);
    }
}

void TablesStatusResponse::read(ReadBuffer & in, UInt64 server_protocol_revision)
{
    if (server_protocol_revision < DBMS_MIN_REVISION_WITH_TABLES_STATUS)
        throw Exception(
                "method TablesStatusResponse::read is called for unsupported server revision",
                ErrorCodes::LOGICAL_ERROR);

    size_t size = 0;
    readVarUInt(size, in);

    if (size > DEFAULT_MAX_STRING_SIZE)
        throw Exception("Too large collection size.", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

    for (size_t i = 0; i < size; ++i)
    {
        QualifiedTableName table_name;
        readBinary(table_name.database, in);
        readBinary(table_name.table, in);

        TableStatus status;
        status.read(in);
        table_states_by_id.emplace(std::move(table_name), std::move(status));
    }
}

}
