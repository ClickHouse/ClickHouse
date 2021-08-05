#pragma once

#include <Interpreters/Context_fwd.h>

#include <cstddef>

namespace DB
{

struct Settings;

struct ExpressionActionsSettings
{
    bool compile_expressions = false;
    size_t min_count_to_compile_expression = 0;

    size_t max_temporary_columns = 0;
    size_t max_temporary_non_const_columns = 0;

    static ExpressionActionsSettings fromSettings(const Settings & from);
    static ExpressionActionsSettings fromContext(ContextPtr from);
};

}
