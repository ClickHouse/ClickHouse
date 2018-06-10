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

/// Some virtual columns routines
namespace
{

bool hasColumn(const ColumnsWithTypeAndName & columns, const String & column_name)
{
    for (const auto & column : columns)
    {
        if (column.name == column_name)
            return true;
    }

    return false;
}


NameAndTypePair tryGetColumn(const ColumnsWithTypeAndName & columns, const String & column_name)
{
    for (const auto & column : columns)
    {
        if (column.name == column_name)
            return {column.name, column.type};
    }

    return {};
}


struct VirtualColumnsProcessor
{
    explicit VirtualColumnsProcessor(const ColumnsWithTypeAndName & all_virtual_columns_)
        : all_virtual_columns(all_virtual_columns_), virtual_columns_mask(all_virtual_columns_.size(), 0) {}

    /// Separates real and virtual column names, returns real ones
    Names process(const Names & column_names, const std::vector<bool *> & virtual_columns_exists_flag = {})
    {
        Names real_column_names;

        if (!virtual_columns_exists_flag.empty())
        {
            for (size_t i = 0; i < all_virtual_columns.size(); ++i)
                *virtual_columns_exists_flag.at(i) = false;
        }

        for (const String & column_name : column_names)
        {
            ssize_t virtual_column_index = -1;

            for (size_t i = 0; i < all_virtual_columns.size(); ++i)
            {
                if (column_name == all_virtual_columns[i].name)
                {
                    virtual_column_index = i;
                    break;
                }
            }

            if (virtual_column_index >= 0)
            {
                auto index = static_cast<size_t>(virtual_column_index);
                virtual_columns_mask[index] = 1;
                if (!virtual_columns_exists_flag.empty())
                    *virtual_columns_exists_flag.at(index) = true;
            }
            else
            {
                real_column_names.emplace_back(column_name);
            }
        }

        return real_column_names;
    }

    void appendVirtualColumns(Block & block)
    {
        for (size_t i = 0; i < all_virtual_columns.size(); ++i)
        {
            if (virtual_columns_mask[i])
                block.insert(all_virtual_columns[i].cloneEmpty());
        }
    }

protected:
    const ColumnsWithTypeAndName & all_virtual_columns;
    std::vector<UInt8> virtual_columns_mask;
};

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
    }));

    virtual_columns =
    {
        {std::make_shared<DataTypeDateTime>(), "metadata_modification_time"},
        {std::make_shared<DataTypeString>(), "create_table_query"},
        {std::make_shared<DataTypeString>(), "engine_full"}
    };
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

    Names real_column_names;
    bool has_metadata_modification_time = false;
    bool has_create_table_query = false;
    bool has_engine_full = false;

    VirtualColumnsProcessor virtual_columns_processor(virtual_columns);
    real_column_names = virtual_columns_processor.process(column_names, {&has_metadata_modification_time, &has_create_table_query, &has_engine_full});
    check(real_column_names);

    Block res_block = getSampleBlock();
    virtual_columns_processor.appendVirtualColumns(res_block);

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

            size_t j = 0;
            res_columns[j++]->insert(database_name);
            res_columns[j++]->insert(table_name);
            res_columns[j++]->insert(iterator->table()->getName());
            res_columns[j++]->insert(UInt64(0));
            res_columns[j++]->insert(iterator->table()->getDataPath());
            res_columns[j++]->insert(database->getTableMetadataPath(table_name));

            if (has_metadata_modification_time)
                res_columns[j++]->insert(static_cast<UInt64>(database->getTableMetadataModificationTime(context, table_name)));

            if (has_create_table_query || has_engine_full)
            {
                ASTPtr ast = database->tryGetCreateTableQuery(context, table_name);

                if (has_create_table_query)
                    res_columns[j++]->insert(ast ? queryToString(ast) : "");

                if (has_engine_full)
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

                    res_columns[j++]->insert(engine_full);
                }
            }
        }
    }

    if (context.hasSessionContext())
    {
        Tables external_tables = context.getSessionContext().getExternalTables();

        for (auto table : external_tables)
        {
            size_t j = 0;
            res_columns[j++]->insertDefault();
            res_columns[j++]->insert(table.first);
            res_columns[j++]->insert(table.second->getName());
            res_columns[j++]->insert(UInt64(1));
            res_columns[j++]->insertDefault();
            res_columns[j++]->insertDefault();

            if (has_metadata_modification_time)
                res_columns[j++]->insertDefault();

            if (has_create_table_query)
                res_columns[j++]->insertDefault();

            if (has_engine_full)
                res_columns[j++]->insert(table.second->getName());
        }
    }

    res_block.setColumns(std::move(res_columns));
    return {std::make_shared<OneBlockInputStream>(res_block)};
}

bool StorageSystemTables::hasColumn(const String & column_name) const
{
    return DB::hasColumn(virtual_columns, column_name) || ITableDeclaration::hasColumn(column_name);
}

NameAndTypePair StorageSystemTables::getColumn(const String & column_name) const
{
    auto virtual_column = DB::tryGetColumn(virtual_columns, column_name);
    return !virtual_column.name.empty() ? virtual_column : ITableDeclaration::getColumn(column_name);
}

}
