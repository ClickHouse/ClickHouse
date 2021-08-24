#pragma once

#include <common/logger_useful.h>
#include <common/shared_ptr_helper.h>
#include <Storages/IStorage.h>
#include <IO/CompressionMethod.h>


namespace DB
{
/**
 * This class represents table engine for external executable files.
 */
class StorageExecutable final : public shared_ptr_helper<StorageExecutable>, public IStorage
{
    friend struct shared_ptr_helper<StorageExecutable>;
public:
    String getName() const override { return "Executable"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned threads) override;

protected:
    StorageExecutable(
        const StorageID & table_id,
        const String & file_path_,
        const String & format_,
        const ColumnsDescription & columns,
        const ConstraintsDescription & constraints);

private:
    String file_path;
    String format;
    Poco::Logger * log;
};
}

