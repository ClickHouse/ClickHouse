#include "config.h"

#if USE_MYSQL
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Databases/MySQL/FetchTablesColumnsList.h>
#include <DataTypes/convertMySQLDataType.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sources/MySQLSource.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <memory>

namespace
{
using namespace DB;

String toQueryStringWithQuote(const std::vector<String> & quote_list)
{
    WriteBufferFromOwnString quote_list_query;
    quote_list_query << "(";

    for (size_t index = 0; index < quote_list.size(); ++index)
    {
        if (index)
            quote_list_query << ",";

        quote_list_query << quote << quote_list[index];
    }

    quote_list_query << ")";
    return quote_list_query.str();
}
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool external_table_functions_use_nulls;
}

std::map<String, ColumnsDescription> fetchTablesColumnsList(
        mysqlxx::PoolWithFailover & pool,
        const String & database_name,
        const std::vector<String> & tables_name,
        const Settings & settings,
        MultiEnum<MySQLDataTypesSupport> type_support)
{
    std::map<String, ColumnsDescription> tables_and_columns;

    if (tables_name.empty())
        return tables_and_columns;

    Block tables_columns_sample_block
    {
        { std::make_shared<DataTypeString>(),   "table_name" },
        { std::make_shared<DataTypeString>(),   "column_name" },
        { std::make_shared<DataTypeString>(),   "column_type" },
        { std::make_shared<DataTypeUInt8>(),    "is_nullable" },
        { std::make_shared<DataTypeUInt8>(),    "is_unsigned" },
        { std::make_shared<DataTypeUInt64>(),   "length" },
        { std::make_shared<DataTypeUInt64>(),   "precision" },
        { std::make_shared<DataTypeUInt64>(),   "scale" },
        { std::make_shared<DataTypeString>(),   "column_comment" },
    };

    WriteBufferFromOwnString query;
    query << "SELECT "
             " TABLE_NAME AS table_name,"
             " COLUMN_NAME AS column_name,"
             " COLUMN_TYPE AS column_type,"
             " IS_NULLABLE = 'YES' AS is_nullable,"
             " COLUMN_TYPE LIKE '%unsigned' AS is_unsigned,"
             " CHARACTER_MAXIMUM_LENGTH AS length,"
             " NUMERIC_PRECISION AS numeric_precision,"
             " IF(ISNULL(NUMERIC_SCALE), DATETIME_PRECISION, NUMERIC_SCALE) AS scale," // we know DATETIME_PRECISION as a scale in CH
             " COLUMN_COMMENT AS column_comment"
             " FROM INFORMATION_SCHEMA.COLUMNS"
             " WHERE ";

    if (!database_name.empty())
        query << " TABLE_SCHEMA = " << quote << database_name << " AND ";

    query << " TABLE_NAME IN " << toQueryStringWithQuote(tables_name) << " ORDER BY ORDINAL_POSITION";

    StreamSettings mysql_input_stream_settings(settings);
    auto result = std::make_unique<MySQLSource>(pool.get(), query.str(), tables_columns_sample_block, mysql_input_stream_settings);
    QueryPipeline pipeline(std::move(result));

    Block block;
    PullingPipelineExecutor executor(pipeline);
    while (executor.pull(block))
    {
        const auto & table_name_col = *block.getByPosition(0).column;
        const auto & column_name_col = *block.getByPosition(1).column;
        const auto & column_type_col = *block.getByPosition(2).column;
        const auto & is_nullable_col = *block.getByPosition(3).column;
        const auto & is_unsigned_col = *block.getByPosition(4).column;
        const auto & char_max_length_col = *block.getByPosition(5).column;
        const auto & precision_col = *block.getByPosition(6).column;
        const auto & scale_col = *block.getByPosition(7).column;
        const auto & column_comment_col = *block.getByPosition(8).column;

        size_t rows = block.rows();
        for (size_t i = 0; i < rows; ++i)
        {
            String table_name = table_name_col[i].safeGet<String>();
            ColumnDescription column_description(
                column_name_col[i].safeGet<String>(),
                convertMySQLDataType(
                    type_support,
                    column_type_col[i].safeGet<String>(),
                    settings[Setting::external_table_functions_use_nulls] && is_nullable_col[i].safeGet<UInt64>(),
                    is_unsigned_col[i].safeGet<UInt64>(),
                    char_max_length_col[i].safeGet<UInt64>(),
                    precision_col[i].safeGet<UInt64>(),
                    scale_col[i].safeGet<UInt64>()));
            column_description.comment = column_comment_col[i].safeGet<String>();

            tables_and_columns[table_name].add(column_description);
        }
    }
    return tables_and_columns;
}

}

#endif
