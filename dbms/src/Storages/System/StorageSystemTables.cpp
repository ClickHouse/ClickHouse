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


BlockInputStreams StorageSystemTables::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    processed_stage = QueryProcessingStage::FetchColumns;

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

    MutableColumns res_columns = res_block.cloneEmptyColumns();

    ColumnPtr filtered_databases_column = getFilteredDatabases(query_info.query, context);

    for (size_t row_number = 0; row_number < filtered_databases_column->size(); ++row_number)
    {
        std::string database_name = filtered_databases_column->getDataAt(row_number).toString();

        auto database = context.tryGetDatabase(database_name);

        if (!database)
        {
            /// Database was deleted just now.
            continue;
        }

        for (auto iterator = database->getIterator(context); iterator->isValid(); iterator->next())
        {
            auto table_name = iterator->name();

            size_t src_index = 0;
            size_t res_index = 0;

            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(database_name);

            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(table_name);

            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(iterator->table()->getName());

            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(UInt64(0));

            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(iterator->table()->getDataPath());

            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(database->getTableMetadataPath(table_name));

            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(static_cast<UInt64>(database->getTableMetadataModificationTime(context, table_name)));

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

    /// This is for temporary tables.

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
                res_columns[res_index++]->insert(table.second->getName());
        }
    }

    res_block.setColumns(std::move(res_columns));
    return {std::make_shared<OneBlockInputStream>(res_block)};
}

}
