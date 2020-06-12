#pragma once

#include <Interpreters/BloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{

class MergeTreeIndexGranuleBloomFilter : public IMergeTreeIndexGranule
{
public:
    MergeTreeIndexGranuleBloomFilter(size_t bits_per_row_, size_t hash_functions_, size_t index_columns_);

    MergeTreeIndexGranuleBloomFilter(size_t bits_per_row_, size_t hash_functions_, size_t total_rows_, const Blocks & granule_index_blocks_);

    bool empty() const override;

    void serializeBinary(WriteBuffer & ostr) const override;

    void deserializeBinary(ReadBuffer & istr) override;

    const std::vector<BloomFilterPtr> & getFilters() const { return bloom_filters; }

private:
    size_t total_rows;
    size_t bits_per_row;
    size_t hash_functions;
    std::vector<BloomFilterPtr> bloom_filters;

    void fillingBloomFilter(BloomFilterPtr & bf, const Block & granule_index_block, size_t index_hash_column) const;
};


}
