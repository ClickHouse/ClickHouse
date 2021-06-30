#pragma once

#include "Coverage.h"

//NOLINTNEXTLINE(bugprone-reserved-identifier, readability-non-const-parameter)
extern "C" void __sanitizer_cov_pcs_init(const uintptr_t * pc_array, const uintptr_t * pc_array_end)
{
    coverage::Writer::instance().initializeRuntime(pc_array, pc_array_end);
}

//NOLINTNEXTLINE(bugprone-reserved-identifier)
extern "C" void __sanitizer_cov_bool_flag_init(bool * start, bool * end) {
    coverage::Writer::instance().hitArray(start, end);
}
