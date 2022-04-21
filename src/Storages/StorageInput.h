#pragma once

#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>

namespace DB
{
/** Internal temporary storage for table function input(...)
  */

class StorageInput final : public IStorage
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageInput> create(TArgs &&... args)
    {
        return std::make_shared<StorageInput>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageInput(CreatePasskey, TArgs &&... args) : StorageInput{std::forward<TArgs>(args)...}
    {
    }

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
