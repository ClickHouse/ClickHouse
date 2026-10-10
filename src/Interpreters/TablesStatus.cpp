#include <Interpreters/TablesStatus.h>
#include <Common/StringWithMemoryTracking.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/ProtocolDefines.h>

#include <algorithm>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int LOGICAL_ERROR;
}

void TableStatus::write(WriteBuffer & out, UInt64 client_protocol_revision) const
{
    writeBinary(is_replicated, out);
    if (is_replicated)
    {
        writeVarUInt(absolute_delay, out);
        if (client_protocol_revision >= DBMS_MIN_REVISION_WITH_TABLE_READ_ONLY_CHECK)
            writeVarUInt(is_readonly, out);
    }
}

void TableStatus::read(ReadBuffer & in, UInt64 server_protocol_revision)
{
    absolute_delay = 0;
    readBinary(is_replicated, in);
    if (is_replicated)
    {
        readVarUInt(absolute_delay, in);
        if (server_protocol_revision >= DBMS_MIN_REVISION_WITH_TABLE_READ_ONLY_CHECK)
            readVarUInt(is_readonly, in);
    }
}

void TablesStatusRequest::write(WriteBuffer & out, UInt64 server_protocol_revision) const
{
    if (server_protocol_revision < DBMS_MIN_REVISION_WITH_TABLES_STATUS)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Method TablesStatusRequest::write is called for unsupported server revision");

    writeVarUInt(tables.size(), out);
    for (const auto & table_name : tables)
    {
        writeBinary(table_name.database, out);
        writeBinary(table_name.table, out);
    }
}

std::string TablesStatusRequest::getAuthDigest() const
{
    /// Order-independent, collision-free digest of the requested tables, folded into the interserver
    /// authentication hash so a relayed hash cannot be reused for a different table set. `tables` is
    /// unordered, so sort the per-table encodings before concatenating. Each `database`/`table`
    /// component is length-prefixed (names may contain arbitrary bytes, including NUL, so a plain
    /// separator would not be injective).
    /// The digest duplicates the whole requested set, twice, and on the signed path it is built
    /// before the peer has been authenticated - so these go through the throwing memory tracker too,
    /// for the same reason as `accounted_for_memory_tracker` in `read` below.
    auto append_sized = [](StringWithMemoryTracking & buf, std::string_view s)
    {
        buf += std::to_string(s.size());
        buf += ':';
        buf += s;
    };

    std::vector<StringWithMemoryTracking> entries;
    entries.reserve(tables.size());
    for (const auto & table_name : tables)
    {
        StringWithMemoryTracking entry;
        append_sized(entry, table_name.database);
        append_sized(entry, table_name.table);
        entries.push_back(std::move(entry));
    }
    std::sort(entries.begin(), entries.end());

    StringWithMemoryTracking data;
    append_sized(data, std::to_string(entries.size()));
    for (const auto & entry : entries)
        append_sized(data, entry);
    /// The digest is returned untracked, which is one copy of something the tracker has already
    /// admitted - keeping the signature means the two callers and the unit test are untouched.
    return std::string(data.data(), data.size());
}

void TablesStatusRequest::read(ReadBuffer & in, UInt64 client_protocol_revision, TablesStatusRequestSource source)
{
    if (client_protocol_revision < DBMS_MIN_REVISION_WITH_TABLES_STATUS)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "method TablesStatusRequest::read is called for unsupported client revision");

    const size_t max_tables = source == TablesStatusRequestSource::InterserverPeer
        ? MAX_TABLES_IN_INTERSERVER_STATUS_REQUEST
        : DEFAULT_MAX_STRING_SIZE;

    size_t size = 0;
    readVarUInt(size, in);

    if (size > max_tables)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large collection size (maximum: {}).", max_tables);

    /// Every name byte is appended here as well, and this string - unlike `QualifiedTableName`'s
    /// plain `std::string` fields - allocates through the throwing memory tracker, so the request is
    /// bounded by `max_server_memory_usage` instead of growing the server's RSS silently.
    ///
    /// The accumulation is what has to be accounted for, not each name on its own: a peer can send
    /// `max_tables` names that are each far below the limit and together far above it, and reading
    /// them one at a time into a tracked buffer would never notice. Hence one buffer for the whole
    /// request, kept alive until it has been read.
    ///
    /// The cost is a second copy of the names while `read` runs, so the peak is a small multiple of
    /// what the tracker sees rather than exactly it - the same shape as `TCPHandler`'s query text,
    /// which is tracked for this reason and then parsed into an untracked AST.
    StringWithMemoryTracking accounted_for_memory_tracker;

    for (size_t i = 0; i < size; ++i)
    {
        QualifiedTableName table_name;
        /// Read before the peer is authenticated: do not allocate the declared size up front.
        readStringBinaryGrowing(table_name.database, in);
        readStringBinaryGrowing(table_name.table, in);
        accounted_for_memory_tracker += table_name.database;
        accounted_for_memory_tracker += table_name.table;
        tables.emplace(std::move(table_name));
    }
}

void TablesStatusResponse::write(WriteBuffer & out, UInt64 client_protocol_revision) const
{
    if (client_protocol_revision < DBMS_MIN_REVISION_WITH_TABLES_STATUS)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "method TablesStatusResponse::write is called for unsupported client revision");

    writeVarUInt(table_states_by_id.size(), out);
    for (const auto & kv : table_states_by_id)
    {
        const QualifiedTableName & table_name = kv.first;
        writeBinary(table_name.database, out);
        writeBinary(table_name.table, out);

        const TableStatus & status = kv.second;
        status.write(out, client_protocol_revision);
    }
}

void TablesStatusResponse::read(ReadBuffer & in, UInt64 server_protocol_revision)
{
    if (server_protocol_revision < DBMS_MIN_REVISION_WITH_TABLES_STATUS)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "method TablesStatusResponse::read is called for unsupported server revision");

    size_t size = 0;
    readVarUInt(size, in);

    if (size > DEFAULT_MAX_STRING_SIZE)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large collection size.");

    for (size_t i = 0; i < size; ++i)
    {
        QualifiedTableName table_name;
        readBinary(table_name.database, in);
        readBinary(table_name.table, in);

        TableStatus status;
        status.read(in, server_protocol_revision);
        table_states_by_id.emplace(std::move(table_name), std::move(status));
    }
}

}
