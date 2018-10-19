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
        {"data_path", std::make_shared<DataTypeString>()},
        {"metadata_path", std::make_shared<DataTypeString>()},
        {"metadata_modification_time", std::make_shared<DataTypeDateTime>()},
        {"dependencies_database", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"dependencies_table", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"create_table_query", std::make_shared<DataTypeString>()},
        {"engine_full", std::make_shared<DataTypeString>()}
    }));
}


static ColumnPtr getFilteredDatabases(const ASTPtr & query, const Context & context)
{
    MutableColumnPtr column = ColumnString::create();
    for (const auto & db : context.getDatabases())
        column->insert(db.first);

    Block block { ColumnWithTypeAndName( std::move(column), std::make_shared<DataTypeString>(), "database" ) };
    VirtualColumnUtils::filterBlockWithQuery(query, block, context);
    return block.getByPosition(0).column;
}


class TablesBlockInputStream : public IProfilingBlockInputStream
{
public:
    TablesBlockInputStream(
        std::vector<UInt8> columns_mask,
        Block header,
        size_t max_block_size,
        ColumnPtr databases,
        const Context & context)
        : columns_mask(columns_mask), header(header), max_block_size(max_block_size), databases(std::move(databases)), context(context) {}

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

            /// This is for temporary tables.  They are output in single block regardless to max_block_size.
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
                            res_columns[res_index++]->insert(UInt64(1));

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
                    }
                }

                res.setColumns(std::move(res_columns));
                done = true;
                return res;
            }

            if (!tables_it || !tables_it->isValid())
                tables_it = database->getIterator(context);

            for (; rows_count < max_block_size && tables_it->isValid(); tables_it->next())
            {
                ++rows_count;
                auto table_name = tables_it->name();

                size_t src_index = 0;
                size_t res_index = 0;

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(database_name);

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(table_name);

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(tables_it->table()->getName());

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(UInt64(0));

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(tables_it->table()->getDataPath());

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(database->getTableMetadataPath(table_name));

                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(static_cast<UInt64>(database->getTableMetadataModificationTime(context, table_name)));

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
                            const ASTCreateQuery & ast_create = typeid_cast<const ASTCreateQuery &>(*ast);
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
            }
        }

        res.setColumns(std::move(res_columns));
        return res;
    }
private:
    std::vector<UInt8> columns_mask;
    Block header;
    size_t max_block_size;
    ColumnPtr databases;
    size_t database_idx = 0;
    DatabaseIteratorPtr tables_it;
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
