#pragma once

#include <Storages/IStorage.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{
/** Internal temporary storage for table function input(...)
  */

class StorageInput : public ext::shared_ptr_helper<StorageInput>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageInput>;
public:
    String getName() const override { return "Input"; }
    String getTableName() const override { return table_name; }

    /// A table will read from this stream.
    void setInputStream(BlockInputStreamPtr input_stream_);

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    String table_name;
    BlockInputStreamPtr input_stream;

protected:
    StorageInput(const String & table_name_, const ColumnsDescription & columns_);
};
}
