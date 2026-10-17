#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct ColumnWithTypeAndName;

class KqlFunctionBase : public IFunction
{
public:
    static bool check_condition(const ColumnWithTypeAndName & condition, ContextPtr context, size_t input_rows_count);
};

}
