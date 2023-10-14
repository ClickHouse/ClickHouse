#pragma once

#include <Interpreters/BloomFilter.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Common/HashTable/HashSet.h>

namespace DB
{

class MergeTreeIndexGranuleBloomFilter final : public IMergeTreeIndexGranule
{
public:
    MergeTreeIndexGranuleBloomFilter(size_t bits_per_row_, size_t hash_functions_, size_t index_columns_);

    MergeTreeIndexGranuleBloomFilter(size_t bits_per_row_, size_t hash_functions_, const std::vector<HashSet<UInt64>> & column_hashes);

    bool empty() const override;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    const std::vector<BloomFilterPtr> & getFilters() const { return bloom_filters; }

private:
    size_t total_rows = 0;
    size_t bits_per_row;
    size_t hash_functions;
    std::vector<BloomFilterPtr> bloom_filters;

    void fillingBloomFilter(BloomFilterPtr & bf, const HashSet<UInt64> & hashes) const;
};


}
