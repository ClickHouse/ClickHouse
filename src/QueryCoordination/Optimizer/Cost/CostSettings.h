#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{

struct Settings;

struct CostSettings
{
    /// Weight of an single operation of preliminary sorting or topn step.
    Float64 cost_pre_sorting_operation_weight;

    /// Weight of uniq and uniqExact agg function in merging stage. Uniq and
    /// uniqExact function in merging stage takes long time than one stage
    /// agg in some data quantities, So here we add a coefficient to use one
    /// stage aggregating.
    Float64 cost_merge_agg_uniq_calculation_weight;

    static CostSettings fromSettings(const Settings & from);
    static CostSettings fromContext(ContextPtr from);
};

}
