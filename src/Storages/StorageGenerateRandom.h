#pragma once

#include <optional>
#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{
/* Generates random data for given schema.
 */
class StorageGenerateRandom final : public ext::shared_ptr_helper<StorageGenerateRandom>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageGenerateRandom>;
public:
    std::string getName() const override { return "GenerateRandom"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    UInt64 max_array_length = 10;
    UInt64 max_string_length = 10;
    UInt64 random_seed = 0;

protected:
    StorageGenerateRandom(const StorageID & table_id_, const ColumnsDescription & columns_,
        UInt64 max_array_length, UInt64 max_string_length, std::optional<UInt64> random_seed);
};

}
