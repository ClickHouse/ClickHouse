#pragma once
#include <sanitizer/coverage_interface.h>

void dumpCoverageReportIfPossible();
void updateTestId(int new_test_id);
void updateReportFile(const char * coverage_report_file);

extern "C" void __sanitizer_cov_trace_pc_guard_init(uint32_t *start, uint32_t *stop);
extern "C" void __sanitizer_cov_trace_pc_guard(uint32_t *edge_index);
