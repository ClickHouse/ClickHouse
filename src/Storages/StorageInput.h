#pragma once

#include <Storages/IStorage.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{
/** Internal temporary storage for table function input(...)
  */

class StorageInput final : public ext::shared_ptr_helper<StorageInput>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageInput>;
public:
    String getName() const override { return "Input"; }

    /// A table will read from this stream.
    void setInputStream(BlockInputStreamPtr input_stream_);

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    BlockInputStreamPtr input_stream;

protected:
    StorageInput(const StorageID & table_id, const ColumnsDescription & columns_);
};
}
