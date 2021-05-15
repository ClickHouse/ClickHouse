#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_EMBEDDED_COMPILER

#include <Functions/IFunction.h>
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

/** Compile function to native jit code using CHJIT instance.
  * Function is compiled as single module.
  * After this function execution, code for function will be compiled and can be queried using
  * findCompiledFunction with function name.
  * Compiled function can be safely casted to JITCompiledFunction type and must be called with
  * valid ColumnData and ColumnDataRowsSize.
  * It is important that ColumnData parameter of JITCompiledFunction is result column,
  * and will be filled by compiled function.
  */
CHJIT::CompiledModuleInfo compileFunction(CHJIT & jit, const IFunctionBase & function);

}

#endif
