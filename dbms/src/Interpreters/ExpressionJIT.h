#pragma once

#include <Common/config.h>

#if USE_EMBEDDED_COMPILER

#include <Interpreters/ExpressionActions.h>

namespace DB
{

/// For each APPLY_FUNCTION action, try to compile the function to native code; if the only uses of a compilable
/// function's result are as arguments to other compilable functions, inline it and leave the now-redundant action as-is.
void compileFunctions(ExpressionActions::Actions & actions, const Names & output_columns, const Block & sample_block);

}

#endif
