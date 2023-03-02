#pragma once

#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>

namespace DB
{
/** Internal temporary storage for table function input(...)
  */

class StorageInput final : public IStorage
{
public:
    StorageInput(const StorageID & table_id, const ColumnsDescription & columns_);

    String getName() const override { return "Input"; }

    /// A table will read from this stream.
    void setPipe(Pipe pipe_);

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    Pipe pipe;
};
}
