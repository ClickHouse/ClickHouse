#include <Storages/MergeTree/MergeTreeIndexAggregatorBloomFilter.h>

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
    : bits_per_row(bits_per_row_), hash_functions(hash_functions_), index_columns_name(columns_name_), column_hashes(columns_name_.size())
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
    const auto granule = std::make_shared<MergeTreeIndexGranuleBloomFilter>(bits_per_row, hash_functions, column_hashes);
    total_rows = 0;
    column_hashes.clear();
    return granule;
}

void MergeTreeIndexAggregatorBloomFilter::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The provided position is not less than the number of block rows. "
                        "Position: {}, Block rows: {}.", *pos, block.rows());

    Block granule_index_block;
    size_t max_read_rows = std::min(block.rows() - *pos, limit);

    for (size_t column = 0; column < index_columns_name.size(); ++column)
    {
        const auto & column_and_type = block.getByName(index_columns_name[column]);
        auto index_column = BloomFilterHash::hashWithColumn(column_and_type.type, column_and_type.column, *pos, max_read_rows);

        const auto & index_col = checkAndGetColumn<ColumnUInt64>(index_column.get());
        const auto & index_data = index_col->getData();
        for (const auto & hash: index_data)
            column_hashes[column].insert(hash);
    }

    *pos += max_read_rows;
    total_rows += max_read_rows;
}

}
