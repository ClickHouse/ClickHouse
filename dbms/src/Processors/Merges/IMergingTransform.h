#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

/// Base class for merging transforms.
class IMergingTransform : public IProcessor
{
public:
    IMergingTransform(size_t num_inputs, const Block & input_header, const Block & output_header, bool have_all_inputs);

    /// Methods to add additional input port. It is possible to do only before the first call of `prepare`.
    void addInput();
    /// Need to be called after all inputs are added. (only if have_all_inputs was not specified).
    void setHaveAllInputs();

    Status prepare() override;

protected:

    virtual void onNewInput(); /// Is called when new input is added. To initialize input's data.
    virtual void initializeInputs() = 0; /// Is called after first chunk was read for every input.
    virtual void consume(Chunk chunk, size_t input_number) = 0; /// Is called after chunk was consumed from input.

    void requestDataForInput(size_t input_number); /// Call it to say that next chunk of data is required for input.
    void finish() { is_finished = true; } /// Call it when all data was inserted to merged_data.

    /// Struct which represents current merging chunk of data.
    /// Also it calculates the number of merged rows.
    class MergedData
    {
    public:
        explicit MergedData(const Block & header)
        {
            columns.reserve(header.columns());
            for (const auto & column : header)
                columns.emplace_back(column.type->createColumn());
        }

        void insertRow(const ColumnRawPtrs & raw_columns, size_t row)
        {
            size_t num_columns = raw_columns.size();
            for (size_t i = 0; i < num_columns; ++i)
                columns[i]->insertFrom(*raw_columns[i], row);

            ++total_merged_rows;
            ++merged_rows;
        }

        void insertFromChunk(Chunk && chunk, size_t limit_rows)
        {
            if (merged_rows)
                throw Exception("Cannot insert to MergedData from Chunk because MergedData is not empty.",
                                ErrorCodes::LOGICAL_ERROR);

            auto num_rows = chunk.getNumRows();
            columns = chunk.mutateColumns();
            if (limit_rows && num_rows > limit_rows)
            {
                num_rows = limit_rows;
                for (auto & column : columns)
                    column = (*column->cut(0, num_rows)).mutate();
            }

            total_merged_rows += num_rows;
            merged_rows = num_rows;
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

            return chunk;
        }

        UInt64 totalMergedRows() const { return total_merged_rows; }
        UInt64 mergedRows() const { return merged_rows; }

    private:
        UInt64 total_merged_rows = 0;
        UInt64 merged_rows = 0;
        MutableColumns columns;
    };

    MergedData merged_data;

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
