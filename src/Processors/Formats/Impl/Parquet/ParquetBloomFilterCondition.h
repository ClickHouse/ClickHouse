#pragma once

#include <config.h>

#if USE_PARQUET

#include <Storages/MergeTree/KeyCondition.h>

namespace parquet
{
class BloomFilter;
}

namespace DB
{

class ParquetBloomFilterCondition
{
public:
    using RPNElement = KeyCondition::RPNElement;
    using IndexColumnToColumnBF = std::unordered_map<std::size_t, std::unique_ptr<parquet::BloomFilter>>;

    explicit ParquetBloomFilterCondition(const std::vector<RPNElement> & rpn_, const std::vector<DataTypePtr> & data_types_);

    bool mayBeTrueOnRowGroup(const IndexColumnToColumnBF & column_index_to_column_bf) const;

private:
    std::vector<RPNElement> rpn;
    std::vector<DataTypePtr> data_types;
};

}

#endif
