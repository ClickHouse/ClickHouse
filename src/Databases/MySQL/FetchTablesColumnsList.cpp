#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL
#include <Core/Block.h>
#include <Databases/MySQL/FetchTablesColumnsList.h>
#include <DataTypes/convertMySQLDataType.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/MySQLBlockInputStream.h>
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

std::map<String, NamesAndTypesList> fetchTablesColumnsList(
        mysqlxx::Pool & pool,
        const String & database_name,
        const std::vector<String> & tables_name,
        bool external_table_functions_use_nulls,
        MultiEnum<MySQLDataTypesSupport> type_support)
{
    std::map<String, NamesAndTypesList> tables_and_columns;

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
    };

    WriteBufferFromOwnString query;
    query << "SELECT "
             " TABLE_NAME AS table_name,"
             " COLUMN_NAME AS column_name,"
             " COLUMN_TYPE AS column_type,"
             " IS_NULLABLE = 'YES' AS is_nullable,"
             " COLUMN_TYPE LIKE '%unsigned' AS is_unsigned,"
             " CHARACTER_MAXIMUM_LENGTH AS length,"
             " NUMERIC_PRECISION as '',"
             " IF(ISNULL(NUMERIC_SCALE), DATETIME_PRECISION, NUMERIC_SCALE) AS scale" // we know DATETIME_PRECISION as a scale in CH
             " FROM INFORMATION_SCHEMA.COLUMNS"
             " WHERE TABLE_SCHEMA = " << quote << database_name
          << " AND TABLE_NAME IN " << toQueryStringWithQuote(tables_name) << " ORDER BY ORDINAL_POSITION";

    MySQLBlockInputStream result(pool.get(), query.str(), tables_columns_sample_block, DEFAULT_BLOCK_SIZE);
    while (Block block = result.read())
    {
        const auto & table_name_col = *block.getByPosition(0).column;
        const auto & column_name_col = *block.getByPosition(1).column;
        const auto & column_type_col = *block.getByPosition(2).column;
        const auto & is_nullable_col = *block.getByPosition(3).column;
        const auto & is_unsigned_col = *block.getByPosition(4).column;
        const auto & char_max_length_col = *block.getByPosition(5).column;
        const auto & precision_col = *block.getByPosition(6).column;
        const auto & scale_col = *block.getByPosition(7).column;

        size_t rows = block.rows();
        for (size_t i = 0; i < rows; ++i)
        {
            String table_name = table_name_col[i].safeGet<String>();
            tables_and_columns[table_name].emplace_back(
                    column_name_col[i].safeGet<String>(),
                    convertMySQLDataType(
                            type_support,
                            column_type_col[i].safeGet<String>(),
                            external_table_functions_use_nulls && is_nullable_col[i].safeGet<UInt64>(),
                            is_unsigned_col[i].safeGet<UInt64>(),
                            char_max_length_col[i].safeGet<UInt64>(),
                            precision_col[i].safeGet<UInt64>(),
                            scale_col[i].safeGet<UInt64>()));
        }
    }
    return tables_and_columns;
}

}

#endif
