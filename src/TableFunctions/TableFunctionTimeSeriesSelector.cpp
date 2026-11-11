#include <TableFunctions/TableFunctionTimeSeriesSelector.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Parsers/ASTFunction.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


void TableFunctionTimeSeriesSelector::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", name);

    auto & args = args_func.arguments->children;
    config = StorageTimeSeriesSelector::getConfiguration(args, context);
}

ColumnsDescription TableFunctionTimeSeriesSelector::getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const
{
    if (config.return_columns == StorageTimeSeriesSelector::Configuration::ReturnColumns::Samples)
    {
        /// The column `samples`: an array of samples per row.
        DataTypePtr samples_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{config.table_timestamp_type, config.table_value_type}));
        return ColumnsDescription({
            {TimeSeriesColumnNames::ID, config.table_id_type},
            {TimeSeriesColumnNames::Samples, samples_type}
        });
    }

    /// The columns `timestamp` and `value`: one sample per row.
    return ColumnsDescription({
        {TimeSeriesColumnNames::ID, config.table_id_type},
        {TimeSeriesColumnNames::Timestamp, config.table_timestamp_type},
        {TimeSeriesColumnNames::Value, config.table_value_type}
    });
}

StoragePtr TableFunctionTimeSeriesSelector::executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr context,
        const String & table_name,
        ColumnsDescription /* cached_columns */,
        bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StorageTimeSeriesSelector>(StorageID(getDatabaseName(), table_name), columns, config);
    res->startup();
    return res;
}

}
