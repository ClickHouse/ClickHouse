#pragma once

#include <config.h>

#if USE_PARQUET

#include <Storages/MergeTree/KeyCondition.h>
#include <Processors/Formats/Impl/ArrowFieldIndexUtil.h>

namespace parquet
{
    class RowGroupMetadata;
}

namespace DB
{

KeyCondition::RPN keyConditionRPNToParquetRPN(const std::vector<KeyCondition::RPNElement> & rpn,
                                              const std::vector<ArrowFieldIndexUtil::ClickHouseIndexToParquetIndex> & clickhouse_column_index_to_parquet_index,
                                              const std::unique_ptr<parquet::RowGroupMetaData> & parquet_rg_metadata);

}

#endif
