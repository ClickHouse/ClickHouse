#pragma once

#include "Coverage.h"

extern "C" void __sanitizer_cov_pcs_init(const uintptr_t * pc_array, const uintptr_t * pc_array_end) //NOLINT
{
    coverage::Writer::instance().pcTableCallback(pc_array, pc_array_end);
}

extern "C" void __sanitizer_cov_bool_flag_init(bool * start, bool * end) //NOLINT
{
    std::fill(start, end, false); 
    coverage::Writer::instance().countersCallback(start, end);
}
