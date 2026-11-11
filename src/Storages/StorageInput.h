#pragma once

#include <QueryPipeline/Pipe.h>
#include <Storages/StorageWithCommonVirtualColumns.h>

namespace DB
{
/** Internal temporary storage for table function input(...)
  */

class StorageInput final : public StorageWithCommonVirtualColumns
{
    friend class ReadFromInput;
public:
    StorageInput(const StorageID & table_id, const ColumnsDescription & columns_);

    String getName() const override { return "Input"; }

    static VirtualColumnsDescription createVirtuals();

    /// A table will read from this stream.
    void setPipe(Pipe pipe_);

    /// A source reading the blocks of data sent by the client, with the input callbacks of the query context.
    /// The input must be initialized with `Context::initializeInput` before.
    static Pipe createInputFromClient(ContextPtr query_context, SharedHeader header);

    void readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    Pipe pipe;
    bool was_pipe_initialized = false;
    bool was_pipe_used = false;
    bool is_input_initialized = false;
};
}
