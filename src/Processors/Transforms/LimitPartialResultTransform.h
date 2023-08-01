#pragma once

#include <Processors/Transforms/PartialResultTransform.h>

namespace DB
{

/// Currently support only single thread implementation with one input and one output ports
class LimitPartialResultTransform : public PartialResultTransform
{
public:
    LimitPartialResultTransform(
        const Block & header,
        UInt64 partial_result_limit_,
        UInt64 partial_result_duration_ms_,
        UInt64 limit_,
        UInt64 offset_)
        : PartialResultTransform(header, partial_result_limit_, partial_result_duration_ms_)
        , limit(limit_)
        , offset(offset_)
        {}

    String getName() const override { return "LimitPartialResultTransform"; }

    void transformPartialResult(Chunk & chunk) override
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
            auto columns = chunk.detachColumns();
            UInt64 num_columns = chunk.getNumColumns();

            for (UInt64 i = 0; i < num_columns; ++i)
                columns[i] = columns[i]->cut(offset, limit);

            chunk.setColumns(std::move(columns), length);
        }
    }

private:
    UInt64 limit;
    UInt64 offset;
};

}
