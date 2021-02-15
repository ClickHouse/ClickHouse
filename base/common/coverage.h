#pragma once

#ifndef CMAKE_CODE_COV
inline void dumpCoverageReportIfPossible() {}
#else
void dumpCoverageReportIfPossible();
#endif
