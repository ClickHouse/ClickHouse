#pragma once
#include <base/types.h>
#include <Core/UUID.h>
#include <Parsers/IAST_fwd.h>
#include <Core/QualifiedTableName.h>

#include <fmt/format.h>

namespace Poco
{
namespace Util
{
class AbstractConfiguration; // NOLINT(cppcoreguidelines-virtual-class-destructor)
}
}

namespace DB
{

static constexpr char const * TABLE_WITH_UUID_NAME_PLACEHOLDER = "_";

class ASTQueryWithTableAndOutput;
class ASTTableIdentifier;
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

    StorageID(const ASTQueryWithTableAndOutput & query); /// NOLINT
    StorageID(const ASTTableIdentifier & table_identifier_node); /// NOLINT
    StorageID(const ASTPtr & node); /// NOLINT

    explicit StorageID(const QualifiedTableName & qualified_name) : StorageID(qualified_name.database, qualified_name.table) { }

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

    bool hasDatabase() const { return !database_name.empty(); }

    bool operator==(const StorageID & rhs) const;

    void assertNotEmpty() const;

    /// Avoid implicit construction of empty StorageID. However, it's needed for deferred initialization.
    static StorageID createEmpty() { return {}; }

    QualifiedTableName getQualifiedName() const { return {database_name, getTableName()}; }

    static StorageID fromDictionaryConfig(const Poco::Util::AbstractConfiguration & config,
                                          const String & config_prefix);

    /// If dictionary has UUID, then use it as dictionary name in ExternalLoader to allow dictionary renaming.
    /// ExternalDictnariesLoader::resolveDictionaryName(...) should be used to access such dictionaries by name.
    String getInternalDictionaryName() const { return getShortName(); }
    /// Get short, but unique, name.
    String getShortName() const;

    /// Calculates hash using only the database and table name of a StorageID.
    struct DatabaseAndTableNameHash
    {
        size_t operator()(const StorageID & storage_id) const;
    };

    /// Checks if the database and table name of two StorageIDs are equal.
    struct DatabaseAndTableNameEqual
    {
        bool operator()(const StorageID & left, const StorageID & right) const
        {
            return (left.database_name == right.database_name) && (left.table_name == right.table_name);
        }
    };

private:
    StorageID() = default;
};

}

namespace fmt
{
    template <>
    struct formatter<DB::StorageID>
    {
        static constexpr auto parse(format_parse_context & ctx)
        {
            return ctx.begin();
        }

        template <typename FormatContext>
        auto format(const DB::StorageID & storage_id, FormatContext & ctx) const
        {
            return fmt::format_to(ctx.out(), "{}", storage_id.getNameForLogs());
        }
    };
}
