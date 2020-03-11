#pragma once
#include <Core/Types.h>
#include <Core/UUID.h>
#include <tuple>
#include <Parsers/IAST_fwd.h>
#include <Core/QualifiedTableName.h>

namespace DB
{

static constexpr char const * TABLE_WITH_UUID_NAME_PLACEHOLDER = "_";

class ASTQueryWithTableAndOutput;
class Context;

struct StorageID
{
    String database_name;
    String table_name;
    UUID uuid = UUIDHelpers::Nil;


    StorageID(const String & database, const String & table, UUID uuid_ = UUIDHelpers::Nil)
            : database_name(database), table_name(table), uuid(uuid_)
    {
        assertNotEmpty();
    }

    StorageID(const ASTQueryWithTableAndOutput & query, const Context & local_context);

    static StorageID resolveFromAST(const ASTPtr & table_identifier_node, const Context & context);

    String getDatabaseName() const;

    String getTableName() const
    {
        assertNotEmpty();
        return table_name;
    }

    String getFullTableName() const;

    String getNameForLogs() const;

    explicit operator bool () const
    {
        return !empty();
    }

    bool empty() const
    {
        return table_name.empty() && !hasUUID();
    }

    bool hasUUID() const
    {
        return uuid != UUID{UInt128(0, 0)};
    }

    bool operator<(const StorageID & rhs) const;

    void assertNotEmpty() const;

    /// Avoid implicit construction of empty StorageID. However, it's needed for deferred initialization.
    static StorageID createEmpty() { return {}; }

    QualifiedTableName getQualifiedName() const { return {database_name, getTableName()}; }

private:
    StorageID() = default;
};

}
