#pragma once
#include <common/types.h>
#include <Core/UUID.h>
#include <tuple>
#include <Parsers/IAST_fwd.h>
#include <Core/QualifiedTableName.h>
#include <Common/Exception.h>

namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
}

static constexpr char const * TABLE_WITH_UUID_NAME_PLACEHOLDER = "_";

class ASTQueryWithTableAndOutput;
class ASTTableIdentifier;
class Context;

// TODO(ilezhankin): refactor and merge |ASTTableIdentifier|
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

    StorageID(const ASTQueryWithTableAndOutput & query);
    StorageID(const ASTTableIdentifier & table_identifier_node);
    StorageID(const ASTPtr & node);

    String getDatabaseName() const;

    String getTableName() const;

    String getFullTableName() const;
    String getFullNameNotQuoted() const;

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
        return uuid != UUIDHelpers::Nil;
    }

    bool operator<(const StorageID & rhs) const;
    bool operator==(const StorageID & rhs) const;

    void assertNotEmpty() const
    {
        // Can be triggered by user input, e.g. SELECT joinGetOrNull('', 'num', 500)
        if (empty())
            throw Exception("Both table name and UUID are empty", ErrorCodes::UNKNOWN_TABLE);
        if (table_name.empty() && !database_name.empty())
            throw Exception("Table name is empty, but database name is not", ErrorCodes::UNKNOWN_TABLE);
    }

    /// Avoid implicit construction of empty StorageID. However, it's needed for deferred initialization.
    static StorageID createEmpty() { return {}; }

    QualifiedTableName getQualifiedName() const { return {database_name, getTableName()}; }

    static StorageID fromDictionaryConfig(const Poco::Util::AbstractConfiguration & config,
                                          const String & config_prefix);

    /// If dictionary has UUID, then use it as dictionary name in ExternalLoader to allow dictionary renaming.
    /// ExternalDictnariesLoader::resolveDictionaryName(...) should be used to access such dictionaries by name.
    String getInternalDictionaryName() const;

private:
    StorageID() = default;
};

}
