#include <Processors/LimitTransform.h>
#include <Processors/Transforms/LimitPartialResultTransform.h>

namespace DB
{

LimitPartialResultTransform::LimitPartialResultTransform(
    const Block & header,
    UInt64 partial_result_limit_,
    UInt64 partial_result_duration_ms_,
    UInt64 limit_,
    UInt64 offset_)
    : PartialResultTransform(header, partial_result_limit_, partial_result_duration_ms_)
    , limit(limit_)
    , offset(offset_)
    {}

void LimitPartialResultTransform::transformPartialResult(Chunk & chunk)
{
    UInt64 num_rows = chunk.getNumRows();
    if (num_rows < offset || limit == 0)
    {
        chunk = {};
        return;
    }

    UInt64 length = std::min(limit, num_rows - offset);

    /// Check if some rows should be removed
    if (length < num_rows)
    {
        UInt64 num_columns = chunk.getNumColumns();
        auto columns = chunk.detachColumns();

        for (UInt64 i = 0; i < num_columns; ++i)
            columns[i] = columns[i]->cut(offset, length);

        chunk.setColumns(std::move(columns), length);
    }
}

}
