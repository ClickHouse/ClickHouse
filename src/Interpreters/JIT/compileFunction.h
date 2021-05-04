#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_EMBEDDED_COMPILER

#include <Functions/IFunctionImpl.h>
#include <Interpreters/JIT/CHJIT.h>

namespace DB
{

struct ColumnData
{
    const char * data = nullptr;
    const char * null = nullptr;
};

ColumnData getColumnData(const IColumn * column);

CHJIT::CompiledModuleInfo compileFunction(CHJIT & jit, const IFunctionBaseImpl & f);

}

#endif
