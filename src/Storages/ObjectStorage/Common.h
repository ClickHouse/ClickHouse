#pragma once

#include <memory>
#include <Storages/IPartitionStrategy.h>

namespace DB
{
struct StorageParsedArguments
{
    String format = "auto";
    String compression_method = "auto";
    /// Set by the parsing paths when the user explicitly supplied a `compression_method`
    /// argument or a named-collection `compression` key. Propagated to
    /// `StorageObjectStorageConfiguration::compression_method_user_provided`.
    bool compression_method_user_provided = false;
    /// The key-value `compression` argument. Unlike the named-collection key, it is not an alias of
    /// `compression_method` and never selects a codec; only the data lake check reads it.
    String ignored_compression_argument;
    String structure = "auto";
    PartitionStrategyFactory::StrategyType partition_strategy_type = PartitionStrategyFactory::StrategyType::NONE;
    /// Whether `partition_strategy_type` came from an explicit argument or named-collection value.
    bool partition_strategy_was_set = false;
    bool partition_columns_in_data_file = true;
    bool partition_columns_in_data_file_was_set = false;
    std::shared_ptr<IPartitionStrategy> partition_strategy;
    /// Set when a base-URL setting (e.g. `s3_base`) rewrote a relative URL coming from a named
    /// collection, so that the resolved URL can be materialized back into the persisted engine args.
    String url_overridden_by_base_setting;
};
}
