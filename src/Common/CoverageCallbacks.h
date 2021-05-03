#pragma once
#include <numeric>
#include "Coverage.h"

//NOLINTNEXTLINE(bugprone-reserved-identifier, readability-non-const-parameter)
extern "C" void __sanitizer_cov_trace_pc_guard_init(uint32_t *start, uint32_t *stop)
{
    if (start == stop || *start) return;
    std::iota(start, stop, 1);
    /// Don't need to notify Writer as we'll get all info from __sanitizer_cov_pcs_init.
}

//NOLINTNEXTLINE(bugprone-reserved-identifier, readability-non-const-parameter)
extern "C" void __sanitizer_cov_trace_pc_guard(uint32_t *edge_index)
{
    if (!*edge_index) return;
    detail::Writer::instance().hit(*edge_index - 1);
}

//NOLINTNEXTLINE(bugprone-reserved-identifier, readability-non-const-parameter)
extern "C" void __sanitizer_cov_pcs_init(const uintptr_t *pcs_beg, const uintptr_t *pcs_end)
{
    detail::Writer::instance().initializePCTable(pcs_beg, pcs_end);
}
