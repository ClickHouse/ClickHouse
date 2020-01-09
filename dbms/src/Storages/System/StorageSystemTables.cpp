#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/System/StorageSystemTables.h>
#include <Storages/VirtualColumnUtils.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>
#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int TABLE_IS_DROPPED;
}


StorageSystemTables::StorageSystemTables(const std::string & name_)
    : name(name_)
{
    setColumns(ColumnsDescription(
    {
        {"database", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"engine", std::make_shared<DataTypeString>()},
        {"is_temporary", std::make_shared<DataTypeUInt8>()},
        {"data_paths", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"metadata_path", std::make_shared<DataTypeString>()},
        {"metadata_modification_time", std::make_shared<DataTypeDateTime>()},
        {"dependencies_database", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"dependencies_table", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"create_table_query", std::make_shared<DataTypeString>()},
        {"engine_full", std::make_shared<DataTypeString>()},
        {"partition_key", std::make_shared<DataTypeString>()},
        {"sorting_key", std::make_shared<DataTypeString>()},
        {"primary_key", std::make_shared<DataTypeString>()},
        {"sampling_key", std::make_shared<DataTypeString>()},
        {"storage_policy", std::make_shared<DataTypeString>()},
    }));
}


static ColumnPtr getFilteredDatabases(const ASTPtr & query, const Context & context)
{
    MutableColumnPtr column = ColumnString::create();
    for (const auto & db : context.getDatabases())
        column->insert(db.first);

    Block block { ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "database") };
    VirtualColumnUtils::filterBlockWithQuery(query, block, context);
    return block.getByPosition(0).column;
}

/// Avoid heavy operation on tables if we only queried columns that we can get without table object.
/// Otherwise it will require table initialization for Lazy database.
static bool needLockStructure(const DatabasePtr & database, const Block & header)
{
    if (database->getEngineName() != "Lazy")
        return true;

    static const std::set<std::string> columns_without_lock = { "database", "name", "metadata_modification_time" };
    for (const auto & column : header.getColumnsWithTypeAndName())
    {
        if (columns_without_lock.find(column.name) == columns_without_lock.end())
            return true;
    }
    return false;
}

class TablesBlockInputStream : public IBlockInputStream
{
public:
    TablesBlockInputStream(
        std::vector<UInt8> columns_mask_,
        Block header_,
        UInt64 max_block_size_,
        ColumnPtr databases_,
        const Context & context_)
        : columns_mask(std::move(columns_mask_))
        , header(std::move(header_))
        , max_block_size(max_block_size_)
        , databases(std::move(databases_))
        , context(context_) {}

    String getName() const override { return "Tables"; }
    Block getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        if (done)
            return {};

        Block res = header;
        MutableColumns res_columns = header.cloneEmptyColumns();

        size_t rows_count = 0;
        while (rows_count < max_block_size)
        {
            if (tables_it && !tables_it->isValid())
                ++database_idx;

            while (database_idx < databases->size() && (!tables_it || !tables_it->isValid()))
            {
                database_name = databases->getDataAt(database_idx).toString();
                database = context.tryGetDatabase(database_name);

                if (!database || !context.hasDatabaseAccessRights(database_name))
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

                    for (auto table : external_tables)
                    {
                        size_t src_index = 0;
                        size_t res_index = 0;

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(table.first);

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(table.second->getName());

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(1u);

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insert(table.second->getName());

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();

                        if (columns_mask[src_index++])
                            res_columns[res_index++]->insertDefault();
                    }
                }

                res.setColumns(std::move(res_columns));
                done = true;
                return res;
            }

            if (!tables_it || !tables_it->isValid())
                tables_it = database->getTablesWithDictionaryTablesIterator(context);

            const bool need_lock_structure = needLockStructure(database, header);

            for (; rows_count < max_block_size && tables_it->isValid(); tables_it->next())
            {
                auto table_name = tables_it->name();
                StoragePtr table = nullptr;

                TableStructureReadLockHolder lock;

                try
                {
                    if (need_lock_structure)
                    {
                        if (!table)
                            table = tables_it->table();
                        lock = table->lockStructureForShare(false, context.getCurrentQueryId());
                    }
                }
                catch (const Exception & e)
                {
                    if (e.code() == ErrorCodes::TABLE_IS_DROPPED)
                        continue;
                    throw;
                }

                ++rows_count;

                size_t src_index = 0;
                size_t res_index = 0;

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(database_name);

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(table_name);

                if (columns_mask[src_index++])
                {
                    if (!table)
                        table = tables_it->table();
                    res_columns[res_index++]->insert(table->getName());
                }

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(0u);  // is_temporary

                if (columns_mask[src_index++])
                {
                    if (!table)
                        table = tables_it->table();

                    Array table_paths_array;
                    auto paths = table->getDataPaths();
                    table_paths_array.reserve(paths.size());
                    for (const String & path : paths)
                        table_paths_array.push_back(path);
                    res_columns[res_index++]->insert(table_paths_array);
                }

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(database->getObjectMetadataPath(table_name));

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(static_cast<UInt64>(database->getObjectMetadataModificationTime(table_name)));

                {
                    Array dependencies_table_name_array;
                    Array dependencies_database_name_array;
                    if (columns_mask[src_index] || columns_mask[src_index + 1])
                    {
                        const auto dependencies = context.getDependencies(database_name, table_name);

                        dependencies_table_name_array.reserve(dependencies.size());
                        dependencies_database_name_array.reserve(dependencies.size());
                        for (const auto & dependency : dependencies)
                        {
                            dependencies_table_name_array.push_back(dependency.second);
                            dependencies_database_name_array.push_back(dependency.first);
                        }
                    }

                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(dependencies_database_name_array);

                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(dependencies_table_name_array);
                }

                if (columns_mask[src_index] || columns_mask[src_index + 1])
                {
                    ASTPtr ast = database->tryGetCreateTableQuery(context, table_name);

                    if (columns_mask[src_index++])
                        res_columns[res_index++]->insert(ast ? queryToString(ast) : "");

                    if (columns_mask[src_index++])
                    {
                        String engine_full;

                        if (ast)
                        {
                            const auto & ast_create = ast->as<ASTCreateQuery &>();
                            if (ast_create.storage)
                            {
                                engine_full = queryToString(*ast_create.storage);

                                static const char * const extra_head = " ENGINE = ";
                                if (startsWith(engine_full, extra_head))
                                    engine_full = engine_full.substr(strlen(extra_head));
                            }
                        }

                        res_columns[res_index++]->insert(engine_full);
                    }
                }
                else
                    src_index += 2;

                ASTPtr expression_ptr;
                if (columns_mask[src_index++])
                {
                    if (!table)
                        table = tables_it->table();
                    if ((expression_ptr = table->getPartitionKeyAST()))
                        res_columns[res_index++]->insert(queryToString(expression_ptr));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (!table)
                        table = tables_it->table();
                    if ((expression_ptr = table->getSortingKeyAST()))
                        res_columns[res_index++]->insert(queryToString(expression_ptr));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (!table)
                        table = tables_it->table();
                    if ((expression_ptr = table->getPrimaryKeyAST()))
                        res_columns[res_index++]->insert(queryToString(expression_ptr));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (!table)
                        table = tables_it->table();
                    if ((expression_ptr = table->getSamplingKeyAST()))
                        res_columns[res_index++]->insert(queryToString(expression_ptr));
                    else
                        res_columns[res_index++]->insertDefault();
                }

                if (columns_mask[src_index++])
                {
                    if (!table)
                        table = tables_it->table();
                    auto policy = table->getStoragePolicy();
                    if (policy)
                        res_columns[res_index++]->insert(policy->getName());
                    else
                        res_columns[res_index++]->insertDefault();
                }
            }
        }

        res.setColumns(std::move(res_columns));
        return res;
    }
private:
    std::vector<UInt8> columns_mask;
    Block header;
    UInt64 max_block_size;
    ColumnPtr databases;
    size_t database_idx = 0;
    DatabaseTablesIteratorPtr tables_it;
    const Context context;
    bool done = false;
    DatabasePtr database;
    std::string database_name;
};


BlockInputStreams StorageSystemTables::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const unsigned /*num_streams*/)
{
    check(column_names);

    /// Create a mask of what columns are needed in the result.

    NameSet names_set(column_names.begin(), column_names.end());

    Block sample_block = getSampleBlock();
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

    ColumnPtr filtered_databases_column = getFilteredDatabases(query_info.query, context);
    return {std::make_shared<TablesBlockInputStream>(
        std::move(columns_mask), std::move(res_block), max_block_size, std::move(filtered_databases_column), context)};
}

}
