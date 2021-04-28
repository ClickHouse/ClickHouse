#pragma once
#include "Coverage.h"

//NOLINTNEXTLINE(bugprone-reserved-identifier, readability-non-const-parameter)
extern "C" void __sanitizer_cov_trace_pc_guard_init(uint32_t *start, uint32_t *stop)
{
    if (start == stop || *start) return;

    static uint32_t n;
    for (uint32_t *edge_index = start; edge_index < stop; edge_index++)
      *edge_index = ++n;

    detail::Writer::instance().initialized(n);
}

//NOLINTNEXTLINE(bugprone-reserved-identifier, readability-non-const-parameter)
extern "C" void __sanitizer_cov_trace_pc_guard(uint32_t *edge_index)
{
    if (!*edge_index) return;
    detail::Writer::instance().hit(__builtin_return_address(0));
}
