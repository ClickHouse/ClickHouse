#include "ColumnFilter.h"
namespace DB
{

void Int64RangeFilter::testInt64Values(DB::RowSet & row_set, size_t offset, size_t len, const Int64 * data)
{
    for (size_t t = 0; t < len; ++t)
    {
        row_set.set(offset + t, data[t] >= min && data[t] <= max);
    }
}
}
