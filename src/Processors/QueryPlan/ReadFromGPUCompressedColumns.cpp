#include <Processors/QueryPlan/ReadFromGPUCompressedColumns.h>

#if USE_GPU

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <GPU/GPUAccumulator.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeCompressedBlockReader.h>
#include <Common/JSONBuilder.h>

#include <atomic>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

namespace
{

using ColumnToReduce = ReadFromGPUCompressedColumns::ColumnToReduce;

struct SharedState
{
    std::vector<ColumnToReduce> columns;
    DataPartsVector parts;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    ReadSettings read_settings;
    size_t batch_bytes;

    std::atomic<size_t> next_part{0};
};

using SharedStatePtr = std::shared_ptr<SharedState>;

class GPUCompressedColumnsSource : public ISource
{
public:
    GPUCompressedColumnsSource(SharedHeader header_, SharedStatePtr state_, QueryStatusPtr query_status_)
        : ISource(std::move(header_))
        , state(std::move(state_))
        , query_status(std::move(query_status_))
    {
    }

    String getName() const override { return "GPUCompressedColumns"; }

protected:
    Chunk generate() override
    {
        const size_t part_idx = state->next_part.fetch_add(1);
        if (part_idx >= state->parts.size())
            return {};

        checkNotCancelled();

        const DataPartPtr & part = state->parts[part_idx];

        MutableColumns result_columns = getPort().getHeader().cloneEmptyColumns();
        if (result_columns.size() != state->columns.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "A row of {} per-part results does not fit an output header of {} columns",
                state->columns.size(),
                result_columns.size());

        for (size_t i = 0; i < state->columns.size(); ++i)
            result_columns[i]->insert(reduceColumn(*part, state->columns[i]));

        return Chunk(std::move(result_columns), 1);
    }

private:
    Field reduceColumn(const IMergeTreeDataPart & part, const ColumnToReduce & column) const
    {
        MergeTreeCompressedBlockReader reader(part, column.column, state->read_settings);

        std::optional<GPU::GPUAccumulator> accumulator;
        size_t rows_read = 0;

        while (const auto block = reader.next())
        {
            if (!accumulator)
            {
                const UInt8 method = *reader.methodByte();
                const auto codec = GPU::codecOf(method);
                if (!codec)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Column {} of part {} is compressed with method {:#x}, which the device cannot expand",
                        column.column.name,
                        part.name,
                        static_cast<UInt16>(method));

                accumulator.emplace(*column.column.type, *column.result_type, column.aggregation, state->batch_bytes, *codec);
            }

            accumulator->addBlock(block->payload, block->compressed_bytes, block->decompressed_bytes);
            rows_read += block->decompressed_bytes / column.column.type->getSizeOfValueInMemory();

            checkNotCancelled();
        }

        if (rows_read != part.rows_count)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Column {} of part {} holds {} values, where the part has {} rows",
                column.column.name,
                part.name,
                rows_read,
                part.rows_count);

        return accumulator->finalize();
    }

    void checkNotCancelled() const
    {
        if (isCancelled())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
        if (query_status)
            query_status->checkTimeLimit();
    }

    SharedStatePtr state;
    QueryStatusPtr query_status;
};

}

ReadFromGPUCompressedColumns::ReadFromGPUCompressedColumns(
    SharedHeader output_header_,
    std::vector<ColumnToReduce> columns_,
    DataPartsVector parts_,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr context_,
    size_t batch_bytes_,
    size_t num_streams_)
    : ISourceStep(std::move(output_header_))
    , columns(std::move(columns_))
    , parts(std::move(parts_))
    , storage_snapshot(std::move(storage_snapshot_))
    , context(std::move(context_))
    , batch_bytes(batch_bytes_)
    , num_streams(num_streams_)
{
}

void ReadFromGPUCompressedColumns::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto state = std::make_shared<SharedState>();
    state->columns = columns;
    state->parts = std::move(parts);
    state->storage_snapshot = storage_snapshot;
    state->context = context;
    state->read_settings = context->getReadSettings();
    state->batch_bytes = batch_bytes;

    const size_t streams = std::max<size_t>(1, std::min(num_streams, state->parts.size()));

    Pipes pipes;
    pipes.reserve(streams);
    for (size_t i = 0; i < streams; ++i)
        pipes.emplace_back(std::make_shared<GPUCompressedColumnsSource>(getOutputHeader(), state, settings.process_list_element));

    auto pipe = Pipe::unitePipes(std::move(pipes));
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

void ReadFromGPUCompressedColumns::describeActions(FormatSettings & format_settings) const
{
    format_settings.out << format_settings.detail_prefix << "Parts: " << parts.size() << "\n";
    format_settings.out << format_settings.detail_prefix << "Columns: ";
    for (size_t i = 0; i < columns.size(); ++i)
        format_settings.out << (i == 0 ? "" : ", ") << columns[i].column.name;
    format_settings.out << "\n";
}

void ReadFromGPUCompressedColumns::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Parts", parts.size());

    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : columns)
        columns_array->add(column.column.name);
    map.add("Columns", std::move(columns_array));
}

}

#endif
