#pragma once
#include <Common/config.h>

#include <Storages/IStorage.h>
#include <IO/CompressionMethod.h>
#include <common/logger_useful.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{
/**
 * This class represents table engine for external executable files.
 */
class StorageExecutable final : public ext::shared_ptr_helper<StorageExecutable>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageExecutable>;
public:
    String getName() const override { return "Executable"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    StorageExecutable(
        const StorageID & table_id,
        const String & file_path_,
        const String & format_,
        BlockInputStreamPtr input_,
        const ColumnsDescription & columns,
        const ConstraintsDescription & constraints,
        const Context & context_);

private:
    String file_path;
    String format;
    BlockInputStreamPtr input;
    const Context & context;
};
}

