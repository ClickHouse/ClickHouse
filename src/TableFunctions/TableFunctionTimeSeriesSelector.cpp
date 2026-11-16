#include <TableFunctions/TableFunctionTimeSeriesSelector.h>

#include <Parsers/ASTFunction.h>
#include <DataTypes/TimeSeries/TimeSeriesColumnNames.h>
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
    using ReturnColumns = StorageTimeSeriesSelector::Configuration::ReturnColumns;
    switch (config.return_columns)
    {
        /// The column `samples`: an array of float samples per row.
        case ReturnColumns::Samples:
            return ColumnsDescription({
                {TimeSeriesColumnNames::ID, config.table_id_type},
                {TimeSeriesColumnNames::Samples, config.table_samples_type}
            });

        /// The column `histograms`: an array of histogram samples per row.
        case ReturnColumns::Histograms:
            return ColumnsDescription({
                {TimeSeriesColumnNames::ID, config.table_id_type},
                {TimeSeriesColumnNames::Histograms, config.table_histograms_type}
            });

        /// Both arrays of the same series within the same bucket: an empty array for a kind without samples there.
        case ReturnColumns::SamplesAndHistograms:
            return ColumnsDescription({
                {TimeSeriesColumnNames::ID, config.table_id_type},
                {TimeSeriesColumnNames::Samples, config.table_samples_type},
                {TimeSeriesColumnNames::Histograms, config.table_histograms_type}
            });

        /// The columns `timestamp` and `value`: one float sample per row.
        case ReturnColumns::TimestampAndValue:
            return ColumnsDescription({
                {TimeSeriesColumnNames::ID, config.table_id_type},
                {TimeSeriesColumnNames::Timestamp, config.table_timestamp_type},
                {TimeSeriesColumnNames::Value, config.table_value_type}
            });
    }
    UNREACHABLE();
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
