#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/System/StorageSystemISTables.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/VirtualColumnUtils.h>
#include <Databases/IDatabase.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>
#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Disks/IStoragePolicy.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>
#include <DataTypes/DataTypeUUID.h>


namespace DB
{

namespace ErrorCodes
{
extern const int TABLE_IS_DROPPED;
}


StorageSystemISTables::StorageSystemISTables(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
        {
            {"table_catalog", std::make_shared<DataTypeString>()},
            //{"table_schema", std::make_shared<DataTypeString>()},
            {"table_name", std::make_shared<DataTypeString>()},
            {"table_type", std::make_shared<DataTypeString>()},
            //{"self_referencing_column_name", std::make_shared<DataTypeString>()},
            //{"reference_generation", std::make_shared<DataTypeString>()},
            //{"user_defined_type_catalog", std::make_shared<DataTypeString>()},
            //{"user_defined_type_schema", std::make_shared<DataTypeString>()},
            //{"user_defined_type_name", std::make_shared<DataTypeString>()},
            {"is_insertable_into", std::make_shared<DataTypeUInt8>()},
            {"is_typed", std::make_shared<DataTypeUInt8>()},
            {"commit_action", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())}
        }));
    setInMemoryMetadata(storage_metadata);
}


static ColumnPtr getFilteredDatabases(const SelectQueryInfo & query_info, const Context & context)
{
    assert(false);
    MutableColumnPtr column = ColumnString::create();

    const auto databases = DatabaseCatalog::instance().getDatabases();
    for (const auto & database_name : databases | boost::adaptors::map_keys)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue; /// We don't want to show the internal database for temporary tables in system.tables

        column->insert(database_name);
    }

    Block block { ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "table_catalog") };
    VirtualColumnUtils::filterBlockWithQuery(query_info.query, block, context);
    return block.getByPosition(0).column;
}

/// Avoid heavy operation on tables if we only queried columns that we can get without table object.
/// Otherwise it will require table initialization for Lazy database.
static bool needLockStructure(const DatabasePtr & database, const Block & header)
{
    assert(false);
    if (database->getEngineName() != "Lazy")
        return true;

    static const std::set<std::string> columns_without_lock = { "table_catalog", "table_name" };
    for (const auto & column : header.getColumnsWithTypeAndName())
    {
        if (columns_without_lock.find(column.name) == columns_without_lock.end())
            return true;
    }
    return false;
}

class TablesBlockSource : public SourceWithProgress
{
public:
    TablesBlockSource(
        std::vector<UInt8> columns_mask_,
        Block header,
        UInt64 max_block_size_,
        ColumnPtr databases_,
        const Context & context_)
        : SourceWithProgress(std::move(header))
        , columns_mask(std::move(columns_mask_))
        , max_block_size(max_block_size_)
        , databases(std::move(databases_))
        , context(context_) {}

    String getName() const override { return "ISTables"; }

protected:
    Chunk generate() override
    {
        assert(false);
        if (done)
            return {};

        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();

        const auto access = context.getAccess();
        const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

        size_t rows_count = 0;
        while (rows_count < max_block_size)
        {
            if (tables_it && !tables_it->isValid())
                ++database_idx;

            while (database_idx < databases->size() && (!tables_it || !tables_it->isValid()))
            {
                database_name = databases->getDataAt(database_idx).toString();
                database = DatabaseCatalog::instance().tryGetDatabase(database_name);

                if (!database)
                {
                    /// Database was deleted just now or the user has no access.
                    ++database_idx;
                    continue;
                }

                break;
            }

            /// This is for temporary tables. They are output in single block regardless to max_block_size.
            if (database_idx >= databases->size())
            {
                if (context.hasSessionContext())
                {
                    Tables external_tables = context.getSessionContext().getExternalTables();

                    for (auto & table : external_tables)
                    {
                        size_t src_index = 0;
                        size_t res_index = 0;

                        // table_catalog
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        // table_name
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(table.second->getName());

                        // table_type
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert("LOCAL TEMPORARY");

                        // is_insertable_into
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(1u);

                        // is_typed
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(0u);

                        // commit_action
                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert("PRESERVE");
                    }
                }

                UInt64 num_rows = res_columns.at(0)->size();
                done = true;
                return Chunk(std::move(res_columns), num_rows);
            }

            const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);

            if (!tables_it || !tables_it->isValid())
                tables_it = database->getTablesIterator(context);

            const bool need_lock_structure = needLockStructure(database, getPort().getHeader());

            for (; rows_count < max_block_size && tables_it->isValid(); tables_it->next())
            {
                auto table_name = tables_it->name();
                if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                    continue;

                StoragePtr table = nullptr;
                TableLockHolder lock;

                if (need_lock_structure)
                {
                    table = tables_it->table();
                    if (table == nullptr)
                    {
                        // Table might have just been removed or detached for Lazy engine (see DatabaseLazy::tryGetTable())
                        continue;
                    }
                    try
                    {
                        lock = table->lockForShare(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
                    }
                    catch (const Exception & e)
                    {
                        if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
                            continue;
                        throw;
                    }
                }

                ++rows_count;

                size_t src_index = 0;
                size_t res_index = 0;

                if (columns_mask[src_index++])
                    //res_columns[res_index++]->insert(database_name);
                    res_columns[res_index++]->insert("system");

                if (columns_mask[src_index++])
                    //res_columns[res_index++]->insert(table_name);
                    res_columns[res_index++]->insert("Hi!");

                if (columns_mask[src_index++])
                    //res_columns[res_index++]->insert("BASE TABLE");
                    res_columns[res_index++]->insert("Hi!");

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(1u);

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(0u);

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insertDefault();


            }
        }

        UInt64 num_rows = res_columns.at(0)->size();
        return Chunk(std::move(res_columns), num_rows);
    }
private:
    std::vector<UInt8> columns_mask;
    UInt64 max_block_size;
    ColumnPtr databases;
    size_t database_idx = 0;
    DatabaseTablesIteratorPtr tables_it;
    const Context context;
    bool done = false;
    DatabasePtr database;
    std::string database_name;
};


Pipe StorageSystemISTables::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const unsigned /*num_streams*/)
{
    assert(false);
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    /// Create a mask of what columns are needed in the result.

    NameSet names_set(column_names.begin(), column_names.end());

    Block sample_block = metadata_snapshot->getSampleBlock();
    Block res_block;

    std::vector<UInt8> columns_mask(sample_block.columns());
    for (size_t i = 0, size = columns_mask.size(); i < size; ++i)
    {
        if (names_set.count(sample_block.getByPosition(i).name))
        {
            columns_mask[i] = 1;
            res_block.insert(sample_block.getByPosition(i));
        }
    }

    ColumnPtr filtered_databases_column = getFilteredDatabases(query_info, context);

    return Pipe(std::make_shared<TablesBlockSource>(
        std::move(columns_mask), std::move(res_block), max_block_size, std::move(filtered_databases_column), context));
}

}
