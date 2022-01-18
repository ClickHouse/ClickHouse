#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_EMBEDDED_COMPILER

#include <Functions/IFunction.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Interpreters/JIT/CHJIT.h>

namespace DB
{

/** ColumnData structure to pass into compiled function.
  * data is raw column data.
  * null_data is null map column raw data.
  */
struct ColumnData
{
    const char * data = nullptr;
    const char * null_data = nullptr;
};

/** Returns ColumnData for column.
  * If constant column is passed, LOGICAL_ERROR will be thrown.
  */
ColumnData getColumnData(const IColumn * column);

using ColumnDataRowsSize = size_t;

using JITCompiledFunction = void (*)(ColumnDataRowsSize, ColumnData *);

struct CompiledFunction
{

    JITCompiledFunction compiled_function;

    CHJIT::CompiledModule compiled_module;
};

/** Compile function to native jit code using CHJIT instance.
  * It is client responsibility to match ColumnData arguments size with
  * function arguments size and additional ColumnData for result.
  */
CompiledFunction compileFunction(CHJIT & jit, const IFunctionBase & function);

struct AggregateFunctionWithOffset
{
    const IAggregateFunction * function;
    size_t aggregate_data_offset;
};

using JITCreateAggregateStatesFunction = void (*)(AggregateDataPtr);
using JITAddIntoAggregateStatesFunction = void (*)(ColumnDataRowsSize, ColumnData *, AggregateDataPtr *);
using JITMergeAggregateStatesFunction = void (*)(AggregateDataPtr, AggregateDataPtr);
using JITInsertAggregateStatesIntoColumnsFunction = void (*)(ColumnDataRowsSize, ColumnData *, AggregateDataPtr *);

struct CompiledAggregateFunctions
{
    JITCreateAggregateStatesFunction create_aggregate_states_function;
    JITAddIntoAggregateStatesFunction add_into_aggregate_states_function;
    JITMergeAggregateStatesFunction merge_aggregate_states_function;
    JITInsertAggregateStatesIntoColumnsFunction insert_aggregates_into_columns_function;

    /// Count of functions that were compiled
    size_t functions_count;

    /// Compiled module. It is client responsibility to destroy it after functions are no longer required.
    CHJIT::CompiledModule compiled_module;
};

/** Compile aggregate function to native jit code using CHJIT instance.
  *
  * JITCreateAggregateStatesFunction will initialize aggregate data ptr with initial aggregate states values.
  * JITAddIntoAggregateStatesFunction will update aggregate states for aggregate functions with specified ColumnData.
  * JITMergeAggregateStatesFunction will merge aggregate states for aggregate functions.
  * JITInsertAggregateStatesIntoColumnsFunction will insert aggregate states for aggregate functions into result columns.
  */
CompiledAggregateFunctions compileAggregateFunctons(CHJIT & jit, const std::vector<AggregateFunctionWithOffset> & functions, std::string functions_dump_name);

}

#endif
