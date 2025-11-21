#pragma once

#include <memory>
#include <Storages/IPartitionStrategy.h>
#include <Common/Exception.h>

namespace DB {
struct StorageParsableArguments
{
    String format = "auto";
    String compression_method = "auto";
    String structure = "auto";
    PartitionStrategyFactory::StrategyType partition_strategy_type = PartitionStrategyFactory::StrategyType::NONE;
    bool partition_columns_in_data_file = true;
    std::shared_ptr<IPartitionStrategy> partition_strategy;
};
}
