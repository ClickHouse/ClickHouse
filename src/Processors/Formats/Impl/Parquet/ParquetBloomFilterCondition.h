#pragma once

#include <config.h>

#if USE_PARQUET

#include <Storages/MergeTree/KeyCondition.h>
#include <parquet/metadata.h>
#include <Processors/Formats/Impl/ArrowFieldIndexUtil.h>

namespace parquet
{
class BloomFilter;
}

namespace DB
{

class ParquetBloomFilterCondition
{
public:

    struct ConditionElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_IN,
            FUNCTION_NOT_IN,
            /// Can take any value.
            FUNCTION_UNKNOWN,
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        using ColumnPtr = IColumn::Ptr;
        using HashesForColumns = std::vector<std::vector<uint64_t>>;
        using KeyColumns = std::vector<std::size_t>;

        Function function;
        // each entry represents a list of hashes per column
        // suppose there are three columns with 2 rows each
        // hashes_per_column.size() == 3 and hashes_per_column[0].size() == 2
        HashesForColumns hashes_per_column;
        KeyColumns key_columns;
    };

    using RPNElement = KeyCondition::RPNElement;
    using ColumnIndexToBF = std::unordered_map<std::size_t, std::unique_ptr<parquet::BloomFilter>>;

    explicit ParquetBloomFilterCondition(const std::vector<ConditionElement> & condition_, const Block & header_);

    bool mayBeTrueOnRowGroup(const ColumnIndexToBF & column_index_to_column_bf) const;
    std::unordered_set<std::size_t> getFilteringColumnKeys() const;

private:
    std::vector<ParquetBloomFilterCondition::ConditionElement> condition;
    Block header;
};

std::vector<ParquetBloomFilterCondition::ConditionElement> keyConditionRPNToParquetBloomFilterCondition(
    const std::vector<KeyCondition::RPNElement> & rpn,
    const std::vector<ArrowFieldIndexUtil::ClickHouseIndexToParquetIndex> & clickhouse_column_index_to_parquet_index,
    const std::unique_ptr<parquet::RowGroupMetaData> & parquet_rg_metadata);

}

#endif
