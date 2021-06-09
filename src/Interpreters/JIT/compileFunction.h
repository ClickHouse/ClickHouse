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
  * Function is compiled as single module.
  * After this function execution, code for function will be compiled and can be queried using
  * findCompiledFunction with function name.
  * Compiled function can be safely casted to JITCompiledFunction type and must be called with
  * valid ColumnData and ColumnDataRowsSize.
  * It is important that ColumnData parameter of JITCompiledFunction is result column,
  * and will be filled by compiled function.
  */
CompiledFunction compileFunction(CHJIT & jit, const IFunctionBase & function);

struct AggregateFunctionWithOffset
{
    const IAggregateFunction * function;
    size_t aggregate_data_offset;
};

using GetAggregateDataContext = char *;
using GetAggregateDataFunction = AggregateDataPtr (*)(GetAggregateDataContext, size_t);

using JITCreateAggregateStatesFunction = void (*)(AggregateDataPtr);
using JITAddIntoAggregateStatesFunction = void (*)(ColumnDataRowsSize, ColumnData *, GetAggregateDataFunction, GetAggregateDataContext);
using JITAddIntoAggregateStatesFunctionV2 = void (*)(ColumnDataRowsSize, ColumnData *, AggregateDataPtr *);
using JITMergeAggregateStatesFunction = void (*)(AggregateDataPtr, AggregateDataPtr);
using JITInsertAggregatesIntoColumnsFunction = void (*)(ColumnDataRowsSize, ColumnData *, AggregateDataPtr *);

struct CompiledAggregateFunctions
{
    JITCreateAggregateStatesFunction create_aggregate_states_function;
    JITAddIntoAggregateStatesFunction add_into_aggregate_states_function;
    JITAddIntoAggregateStatesFunctionV2 add_into_aggregate_states_function_v2;
    JITMergeAggregateStatesFunction merge_aggregate_states_function;
    JITInsertAggregatesIntoColumnsFunction insert_aggregates_into_columns_function;

    size_t functions_count;
    CHJIT::CompiledModule compiled_module;
};

CompiledAggregateFunctions compileAggregateFunctons(CHJIT & jit, const std::vector<AggregateFunctionWithOffset> & functions, std::string functions_dump_name);

}

#endif
