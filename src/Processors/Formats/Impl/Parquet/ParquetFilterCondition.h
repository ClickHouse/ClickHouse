#pragma once

#include <config.h>

#if USE_PARQUET

#include <Storages/MergeTree/KeyCondition.h>
#include <Processors/Formats/Impl/Parquet/ParquetBloomFilterCondition.h>

namespace DB
{

class ParquetFilterCondition
{
public:

    struct BloomFilterData
    {
        using HashesForColumns = std::vector<std::vector<uint64_t>>;
        HashesForColumns hashes_per_column;
        std::vector<std::size_t> key_columns;
    };

    struct ConditionElement : public KeyCondition::RPNElement
    {
        std::optional<BloomFilterData> bloom_filter_data;
    };

    static BoolMask check(const std::vector<ConditionElement> & RPN,
                          const Hyperrectangle & hyperrectangle,
                          const KeyCondition::SpaceFillingCurveDescriptions & key_space_filling_curves,
                          const DataTypes & data_types,
                          const ParquetBloomFilterCondition::ColumnIndexToBF & column_index_to_column_bf,
                          bool single_point);
};

std::vector<ParquetFilterCondition::ConditionElement> abcdefgh(
    const std::vector<KeyCondition::RPNElement> & rpn,
    const std::vector<ArrowFieldIndexUtil::ClickHouseIndexToParquetIndex> & clickhouse_column_index_to_parquet_index,
    const std::unique_ptr<parquet::RowGroupMetaData> & parquet_rg_metadata);

}

#endif
