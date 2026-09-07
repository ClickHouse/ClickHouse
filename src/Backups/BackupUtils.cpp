#include <Access/Common/AccessRightsElement.h>
#include <Backups/BackupUtils.h>
#include <Backups/DDLAdjustingForBackupVisitor.h>
#include <Core/UUID.h>
#include <Databases/DDLRenamingVisitor.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Storages/IStorage.h>
#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>
#include <Common/typeid_cast.h>

#include <optional>

#include "config.h"
#if USE_LIBPQXX
#include <Storages/PostgreSQL/StorageMaterializedPostgreSQL.h>
#endif


namespace DB::BackupUtils
{

DDLRenamingMap makeRenamingMap(const ASTBackupQuery::Elements & elements)
{
    DDLRenamingMap map;

    for (const auto & element : elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::TABLE:
            {
                const String & table_name = element.table_name;
                const String & database_name = element.database_name;
                const String & new_table_name = element.new_table_name;
                const String & new_database_name = element.new_database_name;
                chassert(!table_name.empty());
                chassert(!new_table_name.empty());
                chassert(!database_name.empty());
                chassert(!new_database_name.empty());
                map.setNewTableName({database_name, table_name}, {new_database_name, new_table_name});
                break;
            }

            case ASTBackupQuery::TEMPORARY_TABLE:
            {
                const String & table_name = element.table_name;
                const String & new_table_name = element.new_table_name;
                chassert(!table_name.empty());
                chassert(!new_table_name.empty());
                map.setNewTableName({DatabaseCatalog::TEMPORARY_DATABASE, table_name}, {DatabaseCatalog::TEMPORARY_DATABASE, new_table_name});
                break;
            }

            case ASTBackupQuery::DATABASE:
            {
                const String & database_name = element.database_name;
                const String & new_database_name = element.new_database_name;
                chassert(!database_name.empty());
                chassert(!new_database_name.empty());
                map.setNewDatabaseName(database_name, new_database_name);
                break;
            }

            case ASTBackupQuery::ALL: break;
        }
    }
    return map;
}


/// Returns access required to execute BACKUP query.
AccessRightsElements getRequiredAccessToBackup(const ASTBackupQuery::Elements & elements)
{
    AccessRightsElements required_access;
    for (const auto & element : elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::TABLE:
            {
                required_access.emplace_back(AccessType::BACKUP, element.database_name, element.table_name);
                break;
            }

            case ASTBackupQuery::TEMPORARY_TABLE:
            {
                /// It's always allowed to backup temporary tables.
                break;
            }

            case ASTBackupQuery::DATABASE:
            {
                /// TODO: It's better to process `element.except_tables` somehow.
                required_access.emplace_back(AccessType::BACKUP, element.database_name);
                break;
            }

            case ASTBackupQuery::ALL:
            {
                /// TODO: It's better to process `element.except_databases` & `element.except_tables` somehow.
                required_access.emplace_back(AccessType::BACKUP);
                break;
            }
        }
    }
    return required_access;
}

bool compareRestoredTableDef(const IAST & restored_table_create_query, const IAST & create_query_from_backup, const ContextPtr & global_context)
{
    auto adjust_before_comparison = [&](const IAST & query) -> boost::intrusive_ptr<ASTCreateQuery>
    {
        auto new_query = boost::static_pointer_cast<ASTCreateQuery>(query.clone());
        adjustCreateQueryForBackup(new_query, global_context);
        new_query->resetUUIDs();
        new_query->if_not_exists = false;
        return new_query;
    };

    auto query1 = adjust_before_comparison(restored_table_create_query);
    auto query2 = adjust_before_comparison(create_query_from_backup);
    if (query1->formatWithSecretsOneLine() == query2->formatWithSecretsOneLine())
        return true;

    if (query1->is_time_series_table && query2->is_time_series_table)
    {
        /// Normally queries are stored already normalized in a backup,
        /// but in case there was an upgrade we may need to normalize the queries explicitly here.
        auto normalize_time_series = [&](ASTCreateQuery & query)
        {
            /// Use the same mode as InterpreterCreateQuery uses during RESTORE.
            normalizeTimeSeriesDefinition(query, global_context, LoadingStrictnessLevel::SECONDARY_CREATE, /*is_restore_from_backup=*/true);
        };
        normalize_time_series(*query1);
        normalize_time_series(*query2);
        if (query1->formatWithSecretsOneLine() == query2->formatWithSecretsOneLine())
            return true;
    }

    return false;
}

bool compareRestoredDatabaseDef(const IAST & restored_database_create_query, const IAST & create_query_from_backup, const ContextPtr & global_context)
{
    return compareRestoredTableDef(restored_database_create_query, create_query_from_backup, global_context);
}

bool isInnerTable(const QualifiedTableName & table_name)
{
    return isInnerTable(table_name.database, table_name.table);
}

bool isInnerTable(const String & /* database_name */, const String & table_name)
{
    /// Inner tables of materialized views and of TimeSeries tables. They're backed up through their outer table.
    /// These name prefixes are reserved, so the name alone is enough to tell.
    return table_name.starts_with(".inner.") || table_name.starts_with(".inner_id.")
        || table_name.starts_with(".tmp.inner.") || table_name.starts_with(".tmp.inner_id.");
}

namespace
{

#if USE_LIBPQXX
/// The outer table which a nested table's name points at, reduced to what the classification below needs.
struct OuterTable
{
    String table_name;
    String engine_name;
};

/// The UUID a nested table of this name would have been derived from, or nothing if the name is not even
/// shaped like one.
///
/// A standalone MaterializedPostgreSQL table keeps its rows in a nested table named
/// `<uuid of the outer table>_nested`, see `StorageMaterializedPostgreSQL::getNestedTableName`. That name
/// carries no reserved prefix, and `_nested` is not a reserved suffix either: an ordinary user table may
/// legitimately be called `events_nested`, or even `<some uuid>_nested`. Treating the shape as proof would
/// drop such a table from every backup, so this is only a hint about which table would own a nested table
/// of that name - the answer comes from that outer table's own definition.
std::optional<UUID> outerTableUUIDFromNestedTableName(const String & table_name)
{
    static constexpr std::string_view nested_suffix = StorageMaterializedPostgreSQL::NESTED_TABLE_SUFFIX;
    if (table_name.size() <= nested_suffix.size() || !table_name.ends_with(nested_suffix))
        return {};

    const std::string_view uuid_part{table_name.data(), table_name.size() - nested_suffix.size()};
    /// A nil UUID cannot identify an outer table. Tables of an `Ordinary` database have no UUID at all, so a
    /// MaterializedPostgreSQL table there names its nested table after the nil UUID and cannot be recognised
    /// this way - two of them would collide on that name to begin with. Such a nested table is backed up as a
    /// table of its own, which is what happens on `master` as well.
    UUID outer_uuid;
    if (!tryParseUUID({reinterpret_cast<const UInt8 *>(uuid_part.data()), uuid_part.size()}, outer_uuid)
        || outer_uuid == UUIDHelpers::Nil)
        return {};

    return outer_uuid;
}

/// Whether `table_name` is the nested table of a standalone `MaterializedPostgreSQL` table, given a way to
/// look an outer table up by the UUID the nested table's name is derived from.
///
/// `find_outer_table` returns nothing for a UUID it cannot resolve, and is also where the scope of the
/// lookup is enforced - a nested table sits in the database of the table which named it. It always resolves
/// the outer table inside an enumeration of that database, never in the live `DatabaseCatalog`, so that the
/// answer is a property of the snapshot being backed up rather than of how far a replica has caught up.
template <typename FindOuterTable>
bool isMaterializedPostgreSQLNestedTable(const String & table_name, FindOuterTable && find_outer_table)
{
    auto outer_uuid = outerTableUUIDFromNestedTableName(table_name);
    if (!outer_uuid)
        return false;

    auto outer_table = find_outer_table(*outer_uuid);
    if (!outer_table)
        return false;

    /// The outer table must be the one which named this table, and it must be a standalone
    /// MaterializedPostgreSQL table: that is the engine which derives a nested table name this way.
    ///
    /// `getNestedTableName` returns the storage's own name when it belongs to a MaterializedPostgreSQL
    /// *database* engine, which names its nested tables differently. A table of such a database called
    /// `<its own uuid>_nested` would therefore answer this question about itself, so require the outer
    /// table to be a different table: a nested table is never its own outer table.
    if (outer_table->table_name == table_name)
        return false;

    return outer_table->engine_name == "MaterializedPostgreSQL";
}
#endif

}

std::unordered_set<String> findInnerTables(const std::vector<std::pair<ASTPtr, StoragePtr>> & db_tables)
{
    std::unordered_set<String> inner_tables;

#if USE_LIBPQXX
    std::unordered_map<UUID, const ASTCreateQuery *> create_queries_by_uuid;
    for (const auto & [create_table_query, /* storage */ _] : db_tables)
    {
        const auto * create = create_table_query->as<ASTCreateQuery>();
        if (create && create->uuid != UUIDHelpers::Nil)
            create_queries_by_uuid.emplace(create->uuid, create);
    }

    /// The enumeration is one database, so a table found in it is by construction in the right database.
    auto find_outer_table = [&](const UUID & outer_uuid) -> std::optional<OuterTable>
    {
        auto it = create_queries_by_uuid.find(outer_uuid);
        if (it == create_queries_by_uuid.end())
            return {};

        const auto * outer_create = it->second;
        if (!outer_create->storage || !outer_create->storage->engine)
            return {};

        return OuterTable{outer_create->getTable(), outer_create->storage->engine->name};
    };
#endif

    for (const auto & [create_table_query, /* storage */ _] : db_tables)
    {
        const auto * create = create_table_query->as<ASTCreateQuery>();
        if (!create)
            continue;

        const String table_name = create->getTable();

        if (isInnerTable(create->getDatabase(), table_name))
        {
            inner_tables.emplace(table_name);
            continue;
        }

#if USE_LIBPQXX
        if (isMaterializedPostgreSQLNestedTable(table_name, find_outer_table))
            inner_tables.emplace(table_name);
#endif
    }

    return inner_tables;
}

bool mayBeNestedTableName([[maybe_unused]] const String & table_name)
{
#if USE_LIBPQXX
    return outerTableUUIDFromNestedTableName(table_name).has_value();
#else
    return false;
#endif
}

}
