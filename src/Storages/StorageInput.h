#pragma once

#include <Storages/IStorage.h>
#include <base/shared_ptr_helper.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{
/** Internal temporary storage for table function input(...)
  */

class StorageInput final : public shared_ptr_helper<StorageInput>, public IStorage
{
    friend struct shared_ptr_helper<StorageInput>;
public:
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

protected:
    StorageInput(const StorageID & table_id, const ColumnsDescription & columns_);
};
}
