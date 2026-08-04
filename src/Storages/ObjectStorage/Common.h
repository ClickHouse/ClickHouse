#pragma once

#include <memory>
#include <Storages/IPartitionStrategy.h>

namespace DB
{
struct StorageParsedArguments
{
    String format = "auto";
    String compression_method = "auto";
    String structure = "auto";
    PartitionStrategyFactory::StrategyType partition_strategy_type = PartitionStrategyFactory::StrategyType::NONE;
    bool partition_columns_in_data_file = true;
    bool partition_columns_in_data_file_was_set = false;
    std::shared_ptr<IPartitionStrategy> partition_strategy;
    /// Set when a base-URL setting (e.g. `s3_base`) rewrote a relative URL coming from a named
    /// collection, so that the resolved URL can be materialized back into the persisted engine args.
    String url_overridden_by_base_setting;
};
}
