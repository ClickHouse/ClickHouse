#include <Storages/ReadFromStorageProgress.h>
#include <Processors/ISource.h>

namespace DB
{

void updateRowsProgressApprox(
    ISource & source,
    const Chunk & chunk,
    size_t total_result_size,
    size_t & total_rows_approx_accumulated,
    size_t & total_rows_count_times,
    size_t & total_rows_approx_max)
{
    if (!total_result_size)
        return;

    const size_t num_rows = chunk.getNumRows();
    const auto bytes_per_row = std::ceil(static_cast<double>(chunk.bytes()) / num_rows);
    size_t total_rows_approx = static_cast<size_t>(std::ceil(static_cast<double>(total_result_size) / bytes_per_row));
    total_rows_approx_accumulated += total_rows_approx;
    ++total_rows_count_times;
    total_rows_approx = total_rows_approx_accumulated / total_rows_count_times;

    /// We need to add diff, because total_rows_approx is incremental value.
    /// It would be more correct to send total_rows_approx as is (not a diff),
    /// but incrementation of total_rows_to_read does not allow that.
    /// A new counter can be introduced for that to be sent to client, but it does not worth it.
    if (total_rows_approx > total_rows_approx_max)
    {
        size_t diff = total_rows_approx - total_rows_approx_max;
        source.addTotalRowsApprox(diff);
        total_rows_approx_max = total_rows_approx;
    }
}

}
