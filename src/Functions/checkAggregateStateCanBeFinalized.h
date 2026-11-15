#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>

#include <string_view>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// A column of aggregate states can hold states of a *different* function that shares the same state
/// representation (e.g. `quantileState` vs `quantilesState`) but finalizes to another type. Report that
/// as a normal error instead of tripping a logical error downstream when the column is finalized.
inline void checkAggregateStateCanBeFinalized(
    const ColumnAggregateFunction & column,
    const DataTypePtr & expected_result_type,
    std::string_view function_name)
{
    const auto & actual_result_type = column.getAggregateFunction()->getResultType();
    if (!actual_result_type->equals(*expected_result_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Cannot finalize state of function '{}': it finalizes to {}, but function '{}' is declared to "
            "return {}. States of different aggregate functions that share a state representation cannot "
            "be finalized together.",
            column.getAggregateFunction()->getName(), actual_result_type->getName(),
            function_name, expected_result_type->getName());
}

}
