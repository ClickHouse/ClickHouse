#pragma once
#include <Core/Types.h>

namespace DB
{

class ISource;

void updateRowsProgressApprox(
    ISource & source,
    size_t num_rows,
    UInt64 chunk_bytes_size,
    UInt64 total_result_size,
    UInt64 & total_rows_approx_accumulated,
    size_t & total_rows_count_times,
    UInt64 & total_rows_approx_max);

}
