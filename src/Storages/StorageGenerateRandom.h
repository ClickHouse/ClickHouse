#pragma once

#include <optional>
#include <Storages/IStorage.h>


namespace DB
{
/* Generates random data for given schema.
 */
class StorageGenerateRandom final : public IStorage
{
public:
    StorageGenerateRandom(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const String & comment,
        UInt64 max_array_length,
        UInt64 max_string_length,
        std::optional<UInt64> random_seed);

    std::string getName() const override { return "GenerateRandom"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool supportsTransactions() const override { return true; }
private:
    UInt64 max_array_length = 10;
    UInt64 max_string_length = 10;
    UInt64 random_seed = 0;
};

}
