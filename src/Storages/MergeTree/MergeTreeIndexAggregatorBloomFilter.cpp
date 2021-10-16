#include <Storages/MergeTree/MergeTreeIndexAggregatorBloomFilter.h>

#include <common/bit_cast.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnFixedString.h>
#include <Common/HashTable/Hash.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/BloomFilterHash.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexAggregatorBloomFilter::MergeTreeIndexAggregatorBloomFilter(
    size_t bits_per_row_, size_t hash_functions_, const Names & columns_name_)
    : bits_per_row(bits_per_row_), hash_functions(hash_functions_), index_columns_name(columns_name_)
{
    assert(bits_per_row != 0);
    assert(hash_functions != 0);
}

bool MergeTreeIndexAggregatorBloomFilter::empty() const
{
    return !total_rows;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorBloomFilter::getGranuleAndReset()
{
    const auto granule = std::make_shared<MergeTreeIndexGranuleBloomFilter>(bits_per_row, hash_functions, total_rows, granule_index_blocks);
    total_rows = 0;
    granule_index_blocks.clear();
    return granule;
}

void MergeTreeIndexAggregatorBloomFilter::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception("The provided position is not less than the number of block rows. Position: " + toString(*pos) + ", Block rows: " +
                        toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    Block granule_index_block;
    size_t max_read_rows = std::min(block.rows() - *pos, limit);

    for (const auto & index_column_name : index_columns_name)
    {
        const auto & column_and_type = block.getByName(index_column_name);
        auto index_column = BloomFilterHash::hashWithColumn(column_and_type.type, column_and_type.column, *pos, max_read_rows);

        granule_index_block.insert({index_column, std::make_shared<DataTypeUInt64>(), column_and_type.name});
    }

    *pos += max_read_rows;
    total_rows += max_read_rows;
    granule_index_blocks.push_back(granule_index_block);
}

}
