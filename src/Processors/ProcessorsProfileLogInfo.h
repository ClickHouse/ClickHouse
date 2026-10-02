#pragma once

#include <vector>
#include <base/types.h>

namespace DB
{

/// Information for system.processors_profile_log
struct ProcessorsProfileLogInfo
{
    UInt64 id = 0;
    std::vector<UInt64> parent_ids;
    UInt64 plan_step = 0;
    String plan_step_name;
    String plan_step_description;
    UInt64 plan_group = 0;
    String processor_uniq_id;
    String step_uniq_id;
    String processor_name;
    UInt64 elapsed_us = 0;
    UInt64 input_wait_elapsed_us = 0;
    UInt64 output_wait_elapsed_us = 0;
    UInt64 input_rows = 0;
    UInt64 input_bytes = 0;
    UInt64 output_rows = 0;
    UInt64 output_bytes = 0;
};

}
