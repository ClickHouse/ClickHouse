#include <Storages/MergeTree/rewriteRowsSources.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Defines.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// One part's `_row_exists`, pulled a block at a time as the rewrite reaches the part's rows.
class PartRowExists
{
public:
    explicit PartRowExists(QueryPipeline pipeline_) : pipeline(std::move(pipeline_)), executor(pipeline) {}

    /// Whether the part has a row at `cursor`.
    bool hasRow()
    {
        while (position == block_row_exists.size())
        {
            Chunk chunk;
            if (!executor.pull(chunk))
                return false;
            if (!chunk.hasRows())
                continue;
            const auto column = chunk.getColumns().front()->convertToFullIfWrapped();
            const auto & row_exists = assert_cast<const ColumnUInt8 &>(*column).getData();
            block_row_exists.assign(row_exists.begin(), row_exists.end());
            position = 0;
        }
        return true;
    }

    bool rowExists() const { return block_row_exists[position]; }

    void nextRow()
    {
        ++position;
        ++cursor;
    }

    /// The next row of the part that a row source stands for.
    size_t cursor = 0;

private:
    QueryPipeline pipeline;
    PullingPipelineExecutor executor;
    IColumn::Filter block_row_exists;
    size_t position = 0;
};

}

size_t rewriteRowsSources(
    ReadBuffer & in,
    WriteBuffer & out,
    std::vector<QueryPipeline> row_exists_per_part,
    TemporaryDataBuffer * ttl_kept_rows,
    const MergeTask::GlobalRuntimeContext & global_ctx)
{
    const auto ttl_kept_rows_in = ttl_kept_rows ? ttl_kept_rows->read() : nullptr;

    std::vector<std::unique_ptr<PartRowExists>> parts;
    for (auto & pipeline : row_exists_per_part)
        parts.push_back(std::make_unique<PartRowExists>(std::move(pipeline)));
    size_t num_kept_row_sources = 0;

    auto write_row_source = [&](RowSourcePart row_source)
    {
        if (out.count() % DEFAULT_BLOCK_SIZE == 0)
            global_ctx.checkOperationIsNotCanceled();
        writeBinary(row_source.data, out);
    };

    /// Writes a skipped row source for each dropped row of the part from its cursor on, until the next kept row.
    auto write_dropped_input_rows = [&](size_t part_index)
    {
        auto & part = *parts[part_index];
        for (; part.hasRow() && !part.rowExists(); part.nextRow())
            write_row_source(RowSourcePart(part_index, /*skip_flag=*/ true));
    };

    while (!in.eof())
    {
        RowSourcePart row_source;
        readBinary(row_source.data, in);

        if (!parts.empty())
        {
            const size_t part_index = row_source.getSourceNum();
            chassert(part_index < parts.size(),
                fmt::format("rows_sources of a vertical merge refers to part {} of a merge of {} parts", part_index, parts.size()));
            write_dropped_input_rows(part_index);
            auto & part = *parts[part_index];
            if (!part.hasRow())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "rows_sources of a vertical merge refers to row {} of part {}, past its last row",
                    part.cursor, part_index);
            part.nextRow();
        }

        if (ttl_kept_rows_in && !row_source.getSkipFlag())
        {
            if (ttl_kept_rows_in->eof())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "rows_sources of a vertical merge holds more emitted rows than TTLTransform emitted");

            UInt8 kept = 0;
            readBinary(kept, *ttl_kept_rows_in);
            row_source.setSkipFlag(!kept);
        }

        num_kept_row_sources += !row_source.getSkipFlag();
        write_row_source(row_source);
    }

    for (size_t part_index = 0; part_index < parts.size(); ++part_index)
    {
        write_dropped_input_rows(part_index);
        if (parts[part_index]->hasRow())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "rows_sources of a vertical merge ends at row {} of part {}, which has more rows",
                parts[part_index]->cursor, part_index);
    }

    if (ttl_kept_rows_in && !ttl_kept_rows_in->eof())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "rows_sources of a vertical merge holds fewer emitted rows than TTLTransform emitted");

    return num_kept_row_sources;
}

}
