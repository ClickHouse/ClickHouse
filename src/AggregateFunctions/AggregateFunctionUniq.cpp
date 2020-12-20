#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionUniq.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionSum.h>

#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include "registerAggregateFunctions.h"

#include <Interpreters/Aggregator.h>
#include <Interpreters/ExpressionAnalyzer.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/AggregatingStep.h>

#include <Storages/IStorage_fwd.h>
#include <Storages/IStorage.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{


/** `DataForVariadic` is a data structure that will be used for `uniq` aggregate function of multiple arguments.
  * It differs, for example, in that it uses a trivial hash function, since `uniq` of many arguments first hashes them out itself.
  */
template <typename Data, typename DataForVariadic>
AggregateFunctionPtr createAggregateFunctionUniq(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertNoParameters(name, params);

    if (argument_types.empty())
        throw Exception("Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    bool use_exact_hash_function = !isAllArgumentsContiguousInMemory(argument_types);

    if (argument_types.size() == 1)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniq, Data>(*argument_types[0], argument_types));

        WhichDataType which(argument_type);
        if (res)
            return res;
        else if (which.isDate())
            return std::make_shared<AggregateFunctionUniq<DataTypeDate::FieldType, Data>>(argument_types);
        else if (which.isDateTime())
            return std::make_shared<AggregateFunctionUniq<DataTypeDateTime::FieldType, Data>>(argument_types);
        else if (which.isStringOrFixedString())
            return std::make_shared<AggregateFunctionUniq<String, Data>>(argument_types);
        else if (which.isUUID())
            return std::make_shared<AggregateFunctionUniq<DataTypeUUID::FieldType, Data>>(argument_types);
        else if (which.isTuple())
        {
            if (use_exact_hash_function)
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, true, true>>(argument_types);
            else
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, false, true>>(argument_types);
        }
    }

    /// "Variadic" method also works as a fallback generic case for single argument.
    if (use_exact_hash_function)
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, true, false>>(argument_types);
    else
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, false, false>>(argument_types);
}

template <bool is_exact, template <typename> class Data, typename DataForVariadic>
AggregateFunctionPtr createAggregateFunctionUniq(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertNoParameters(name, params);

    if (argument_types.empty())
        throw Exception("Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// We use exact hash function if the user wants it;
    /// or if the arguments are not contiguous in memory, because only exact hash function have support for this case.
    bool use_exact_hash_function = is_exact || !isAllArgumentsContiguousInMemory(argument_types);

    if (argument_types.size() == 1)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniq, Data>(*argument_types[0], argument_types));

        WhichDataType which(argument_type);
        if (res)
            return res;
        else if (which.isDate())
            return std::make_shared<AggregateFunctionUniq<DataTypeDate::FieldType, Data<DataTypeDate::FieldType>>>(argument_types);
        else if (which.isDateTime())
            return std::make_shared<AggregateFunctionUniq<DataTypeDateTime::FieldType, Data<DataTypeDateTime::FieldType>>>(argument_types);
        else if (which.isStringOrFixedString())
            return std::make_shared<AggregateFunctionUniq<String, Data<String>>>(argument_types);
        else if (which.isUUID())
            return std::make_shared<AggregateFunctionUniq<DataTypeUUID::FieldType, Data<DataTypeUUID::FieldType>>>(argument_types);
        else if (which.isTuple())
        {
            if (use_exact_hash_function)
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, true, true>>(argument_types);
            else
                return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, false, true>>(argument_types);
        }
    }

    /// "Variadic" method also works as a fallback generic case for single argument.
    if (use_exact_hash_function)
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, true, false>>(argument_types);
    else
        return std::make_shared<AggregateFunctionUniqVariadic<DataForVariadic, false, false>>(argument_types);
}

}

void registerAggregateFunctionsUniq(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };

    factory.registerFunction("uniq",
        {createAggregateFunctionUniq<AggregateFunctionUniqUniquesHashSetData, AggregateFunctionUniqUniquesHashSetDataForVariadic>, properties});

    factory.registerFunction("uniqHLL12",
        {createAggregateFunctionUniq<false, AggregateFunctionUniqHLL12Data, AggregateFunctionUniqHLL12DataForVariadic>, properties});

    factory.registerFunction("uniqExact",
        {createAggregateFunctionUniq<true, AggregateFunctionUniqExactData, AggregateFunctionUniqExactData<String>>, properties});
}

template <typename T, typename Data>
void AggregateFunctionUniq<T, Data>::mergeFinalized(
     AggregateDescriptions & merge_aggregates,
     const Block & header_before_aggregation,
     const NamesAndTypesList & aggregation_keys,
     const Names & selected_columns,
     const String & aggregation_column_name)
{
    ColumnNumbers keys;
    for (const auto & key : aggregation_keys)
        keys.push_back(header_before_aggregation.getPositionByName(key.name));
    int distinct_cnt_col_pos = -1;
    int col_size = selected_columns.size();
    for (int i=0; i<col_size; i++) {
        if (selected_columns[i]==aggregation_column_name) {
            distinct_cnt_col_pos = i;
            break;
        }
    }

    if (distinct_cnt_col_pos == -1) {
        throw Exception("the aggregation column name " + aggregation_column_name +
                " is not in the list of selected columns.", ErrorCodes::LOGICAL_ERROR);
    }

    DataTypes types;
    ColumnWithTypeAndName distinct_cnt_col = header_before_aggregation.getByPosition(distinct_cnt_col_pos);
    types.push_back(distinct_cnt_col.type);
    ColumnNumbers args;
    args.push_back(distinct_cnt_col_pos);
    auto agg_func = std::make_shared<AggregateFunctionSum<UInt64, UInt64, AggregateFunctionSumData<UInt64>, AggregateFunctionTypeSum>>(types);
    Array agg_func_params;
    Names arg_names;
    AggregateDescription merge_aggregate{agg_func, agg_func_params, args, arg_names, distinct_cnt_col.name};
    merge_aggregates.push_back(merge_aggregate);
}

}
