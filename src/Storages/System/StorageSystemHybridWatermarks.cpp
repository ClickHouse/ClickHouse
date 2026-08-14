#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/System/StorageSystemHybridWatermarks.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDistributed.h>
#include <Storages/VirtualColumnUtils.h>
#include <Access/ContextAccess.h>
#include <Common/FailPoint.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char hybrid_watermarks_read_fail[];
}


ColumnsDescription StorageSystemHybridWatermarks::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database",       std::make_shared<DataTypeString>(), "Name of the database."},
        {"table",          std::make_shared<DataTypeString>(), "Name of the Hybrid table."},
        {"name",           std::make_shared<DataTypeString>(),
            "Watermark parameter name from hybridParam() (always starts with hybrid_watermark_). Empty on a diagnostic row."},
        {"value",          std::make_shared<DataTypeString>(),
            "Current effective watermark value from the runtime snapshot. Empty on a diagnostic row."},
        {"type",           std::make_shared<DataTypeString>(),
            "Declared type from hybridParam('name', 'type'). Empty on a diagnostic row."},
        {"last_exception", std::make_shared<DataTypeString>(),
            "Empty on success. Populated when reading this table's hybrid metadata raised an exception or produced an inconsistent view."},
    };
}

Block StorageSystemHybridWatermarks::getFilterSampleBlock() const
{
    return {
        { {}, std::make_shared<DataTypeString>(), "database" },
        { {}, std::make_shared<DataTypeString>(), "table" },
    };
}

void StorageSystemHybridWatermarks::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    /// Enumerate only Hybrid tables (StorageDistributed with getName() == "Hybrid").
    std::map<String, std::map<String, StoragePtr>> tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
    {
        if (db.second->isExternal())
            continue;

        /// Temp tables are surfaced via the session-local branch below; mirrors system.tables.
        if (db.first == DatabaseCatalog::TEMPORARY_DATABASE)
            continue;

        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, db.first);

        for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            StoragePtr table = iterator->table();
            if (!table)
                continue;

            const auto * distributed = dynamic_cast<const StorageDistributed *>(table.get());
            if (!distributed || distributed->getName() != "Hybrid")
                continue;

            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, db.first, iterator->name()))
                continue;

            tables[db.first][iterator->name()] = table;
        }
    }

    /// Session-local temporary tables, mirroring `system.tables` (see
    /// StorageSystemTables::read(), the temporary-table branch around line 371).
    /// They are emitted with database = "" to match `system.tables`' convention.
    /// No SHOW_TABLES gate: externals are session-scoped and only visible to the
    /// owning session, the same as in `system.tables`.
    if (context->hasSessionContext())
    {
        for (auto & [name, storage] : context->getSessionContext()->getExternalTables())
        {
            if (!storage)
                continue;

            const auto * distributed = dynamic_cast<const StorageDistributed *>(storage.get());
            if (!distributed || distributed->getName() != "Hybrid")
                continue;

            tables[""][name] = storage;
        }
    }

    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();

    for (auto & db : tables)
    {
        for (auto & table : db.second)
        {
            col_database_mut->insert(db.first);
            col_table_mut->insert(table.first);
        }
    }

    ColumnPtr col_database_to_filter = std::move(col_database_mut);
    ColumnPtr col_table_to_filter = std::move(col_table_mut);

    /// Apply pushed-down predicate on (database, table).
    {
        Block filtered_block
        {
            { col_database_to_filter, std::make_shared<DataTypeString>(), "database" },
            { col_table_to_filter, std::make_shared<DataTypeString>(), "table" },
        };

        VirtualColumnUtils::filterBlockWithPredicate(predicate, filtered_block, context);

        if (!filtered_block.rows())
            return;

        col_database_to_filter = filtered_block.getByName("database").column;
        col_table_to_filter = filtered_block.getByName("table").column;
    }

    auto emit_diagnostic = [&](const String & database, const String & table, const String & message)
    {
        size_t c = 0;
        res_columns[c++]->insert(database);
        res_columns[c++]->insert(table);
        res_columns[c++]->insertDefault(); /// name
        res_columns[c++]->insertDefault(); /// value
        res_columns[c++]->insertDefault(); /// type
        res_columns[c++]->insert(message);
    };

    for (size_t i = 0, tables_size = col_database_to_filter->size(); i < tables_size; ++i)
    {
        String database = (*col_database_to_filter)[i].safeGet<String>();
        String table = (*col_table_to_filter)[i].safeGet<String>();

        auto & distributed_table = dynamic_cast<StorageDistributed &>(*tables[database][table]);

        std::unordered_map<String, String> types;
        MultiVersion<StorageDistributed::WatermarkParams>::Version snapshot;

        /// Per-table fault isolation: mirrors StorageSystemTables (see its lines 450-462).
        /// In normal operation this try never throws, but we want one broken attached
        /// Hybrid table never to take down the whole scan.
        try
        {
            /// Test-only hook for exercising the diagnostic row path from SQL.
            fiu_do_on(FailPoints::hybrid_watermarks_read_fail,
            {
                throw Exception(ErrorCodes::FAULT_INJECTED,
                    "Injected fault for system.hybrid_watermarks");
            });

            types = distributed_table.getDeclaredHybridParamTypes();
            snapshot = distributed_table.getHybridWatermarkParams();
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("StorageSystemHybridWatermarks"),
                fmt::format("Failed to read hybrid watermarks for {}.{}", database, table),
                LogsLevel::information);
            emit_diagnostic(database, table, getCurrentExceptionMessage(/*with_stacktrace=*/false));
            continue;
        }

        /// Zero declared watermarks → emit zero rows.
        if (types.empty())
            continue;

        /// Keyspace mismatch. CREATE enforces declared-keys == snapshot-keys
        /// at [StorageDistributed.cpp], so this only triggers on unexpected runtime drift.
        bool consistent = snapshot && snapshot->size() == types.size();
        if (consistent)
        {
            for (const auto & [name, _] : types)
            {
                if (!snapshot->contains(name))
                {
                    consistent = false;
                    break;
                }
            }
        }

        if (!consistent)
        {
            emit_diagnostic(database, table, fmt::format(
                "Hybrid watermark keyspace mismatch: {} declared, {} in snapshot",
                types.size(), snapshot ? snapshot->size() : 0));
            continue;
        }

        for (const auto & [name, type] : types)
        {
            size_t c = 0;
            res_columns[c++]->insert(database);
            res_columns[c++]->insert(table);
            res_columns[c++]->insert(name);
            res_columns[c++]->insert(snapshot->at(name));
            res_columns[c++]->insert(type);
            res_columns[c++]->insertDefault(); /// last_exception
        }
    }
}

}
