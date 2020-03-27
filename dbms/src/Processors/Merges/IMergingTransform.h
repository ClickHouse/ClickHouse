#pragma once

#include <Processors/IProcessor.h>
#include <Common/Stopwatch.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Base class for merging transforms.
class IMergingTransform : public IProcessor
{
public:
    IMergingTransform(
        size_t num_inputs,
        const Block & input_header,
        const Block & output_header,
        size_t max_block_size,
        bool use_average_block_size,  /// For adaptive granularity. Return chunks with the same avg size as inputs.
        bool have_all_inputs_);

    /// Methods to add additional input port. It is possible to do only before the first call of `prepare`.
    void addInput();
    /// Need to be called after all inputs are added. (only if have_all_inputs was not specified).
    void setHaveAllInputs();

    Status prepare() override;

protected:

    virtual void onNewInput(); /// Is called when new input is added. To initialize input's data.
    virtual void initializeInputs() = 0; /// Is called after first chunk was read for every input.
    virtual void consume(Chunk chunk, size_t input_number) = 0; /// Is called after chunk was consumed from input.
    virtual void onFinish() {} /// Is called when all data is processed.

    void requestDataForInput(size_t input_number); /// Call it to say that next chunk of data is required for input.
    void finish() { is_finished = true; } /// Call it when all data was inserted to merged_data.

    /// Struct which represents current merging chunk of data.
    /// Also it calculates the number of merged rows and other profile info.
    class MergedData
    {
    public:
        explicit MergedData(const Block & header, bool use_average_block_size_, UInt64 max_block_size_)
            : max_block_size(max_block_size_), use_average_block_size(use_average_block_size_)
        {
            columns.reserve(header.columns());
            for (const auto & column : header)
                columns.emplace_back(column.type->createColumn());
        }

        void insertRow(const ColumnRawPtrs & raw_columns, size_t row, size_t block_size)
        {
            size_t num_columns = raw_columns.size();
            for (size_t i = 0; i < num_columns; ++i)
                columns[i]->insertFrom(*raw_columns[i], row);

            ++total_merged_rows;
            ++merged_rows;
            sum_blocks_granularity += block_size;
        }

        void insertFromChunk(Chunk && chunk, size_t limit_rows)
        {
            if (merged_rows)
                throw Exception("Cannot insert to MergedData from Chunk because MergedData is not empty.",
                                ErrorCodes::LOGICAL_ERROR);

            auto num_rows = chunk.getNumRows();
            auto block_size = num_rows;
            columns = chunk.mutateColumns();
            if (limit_rows && num_rows > limit_rows)
            {
                num_rows = limit_rows;
                for (auto & column : columns)
                    column = (*column->cut(0, num_rows)).mutate();
            }

            total_merged_rows += num_rows;
            merged_rows = num_rows;
            sum_blocks_granularity += block_size * num_rows;
        }

        Chunk pull()
        {
            MutableColumns empty_columns;
            empty_columns.reserve(columns.size());

            for (const auto & column : columns)
                empty_columns.emplace_back(column->cloneEmpty());

            empty_columns.swap(columns);
            Chunk chunk(std::move(empty_columns), merged_rows);

            merged_rows = 0;
            sum_blocks_granularity = 0;
            ++total_chunks;
            total_allocated_bytes += chunk.allocatedBytes();

            return chunk;
        }

        bool hasEnoughRows() const
        {
            /// Never return more then max_block_size.
            if (merged_rows >= max_block_size)
                return true;

            if (!use_average_block_size)
                return false;

            /// Zero rows always not enough.
            if (merged_rows == 0)
                return false;

            return merged_rows * merged_rows >= sum_blocks_granularity;
        }

        UInt64 mergedRows() const { return merged_rows; }
        UInt64 totalMergedRows() const { return total_merged_rows; }
        UInt64 totalChunks() const { return total_chunks; }
        UInt64 totalAllocatedBytes() const { return total_allocated_bytes; }

    private:
        MutableColumns columns;

        UInt64 sum_blocks_granularity = 0;
        UInt64 merged_rows = 0;
        UInt64 total_merged_rows = 0;
        UInt64 total_chunks = 0;
        UInt64 total_allocated_bytes = 0;

        const UInt64 max_block_size;
        const bool use_average_block_size;
    };

    MergedData merged_data;

    /// Profile info.
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};

private:
    /// Processor state.
    bool is_initialized = false;
    bool is_finished = false;

    bool need_data = false;
    size_t next_input_to_read = 0;

    std::atomic<bool> have_all_inputs;

    struct InputState
    {
        explicit InputState(InputPort & port_) : port(port_) {}

        InputPort & port;
        bool is_initialized = false;
    };

    std::vector<InputState> input_states;

    Status prepareSingleInput();
    Status prepareInitializeInputs();
};

}
