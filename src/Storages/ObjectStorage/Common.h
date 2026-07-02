#pragma once

#include <memory>
#include <Storages/IPartitionStrategy.h>

namespace DB
{
struct StorageParsedArguments
{
    String format = "auto";
    String compression_method = "auto";
    /// Set by the parsing paths when the user explicitly supplied a
    /// `compression_method`/`compression` argument. Propagated to
    /// `StorageObjectStorageConfiguration::compression_method_user_provided`.
    bool compression_method_user_provided = false;
    String structure = "auto";
    PartitionStrategyFactory::StrategyType partition_strategy_type = PartitionStrategyFactory::StrategyType::NONE;
    bool partition_columns_in_data_file = true;
    bool partition_columns_in_data_file_was_set = false;
    std::shared_ptr<IPartitionStrategy> partition_strategy;
};
}
