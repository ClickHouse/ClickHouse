#include <Storages/MergeTree/MergeTreeIndexGranuleBloomFilter.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/HashTable/Hash.h>
#include <common/bit_cast.h>
#include <Interpreters/BloomFilterHash.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void assertGranuleBlocksStructure(const Blocks & granule_index_blocks)
{
    Block prev_block;
    for (size_t index = 0; index < granule_index_blocks.size(); ++index)
    {
        Block granule_index_block = granule_index_blocks[index];

        if (index != 0)
            assertBlocksHaveEqualStructure(prev_block, granule_index_block, "Granule blocks of bloom filter has difference structure.");

        prev_block = granule_index_block;
    }
}


MergeTreeIndexGranuleBloomFilter::MergeTreeIndexGranuleBloomFilter(size_t bits_per_row_, size_t hash_functions_, size_t index_columns_)
    : bits_per_row(bits_per_row_), hash_functions(hash_functions_)
{
    total_rows = 0;
    bloom_filters.resize(index_columns_);
}

MergeTreeIndexGranuleBloomFilter::MergeTreeIndexGranuleBloomFilter(
    size_t bits_per_row_, size_t hash_functions_, size_t total_rows_, const Blocks & granule_index_blocks_)
        : total_rows(total_rows_), bits_per_row(bits_per_row_), hash_functions(hash_functions_)
{
    if (granule_index_blocks_.empty() || !total_rows)
        throw Exception("LOGICAL ERROR: granule_index_blocks empty or total_rows is zero.", ErrorCodes::LOGICAL_ERROR);

    assertGranuleBlocksStructure(granule_index_blocks_);

    for (size_t index = 0; index < granule_index_blocks_.size(); ++index)
    {
        Block granule_index_block = granule_index_blocks_[index];

        if (unlikely(!granule_index_block || !granule_index_block.rows()))
            throw Exception("LOGICAL ERROR: granule_index_block is empty.", ErrorCodes::LOGICAL_ERROR);

        if (index == 0)
        {
            static size_t atom_size = 8;

            for (size_t column = 0, columns = granule_index_block.columns(); column < columns; ++column)
            {
                size_t total_items = total_rows;

                if (const auto * array_col = typeid_cast<const ColumnArray *>(granule_index_block.getByPosition(column).column.get()))
                {
                    const IColumn * nested_col = array_col->getDataPtr().get();
                    total_items = nested_col->size();
                }

                size_t bytes_size = (bits_per_row * total_items + atom_size - 1) / atom_size;
                bloom_filters.emplace_back(std::make_shared<BloomFilter>(bytes_size, hash_functions, 0));
            }
        }

        for (size_t column = 0, columns = granule_index_block.columns(); column < columns; ++column)
            fillingBloomFilter(bloom_filters[column], granule_index_block, column);
    }
}

bool MergeTreeIndexGranuleBloomFilter::empty() const
{
    return !total_rows;
}

void MergeTreeIndexGranuleBloomFilter::deserializeBinary(ReadBuffer & istr)
{
    if (!empty())
        throw Exception("Cannot read data to a non-empty bloom filter index.", ErrorCodes::LOGICAL_ERROR);

    readVarUInt(total_rows, istr);
    for (auto & filter : bloom_filters)
    {
        static size_t atom_size = 8;
        size_t bytes_size = (bits_per_row * total_rows + atom_size - 1) / atom_size;
        filter = std::make_shared<BloomFilter>(bytes_size, hash_functions, 0);
        istr.read(reinterpret_cast<char *>(filter->getFilter().data()), bytes_size);
    }
}

void MergeTreeIndexGranuleBloomFilter::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception("Attempt to write empty bloom filter index.", ErrorCodes::LOGICAL_ERROR);

    static size_t atom_size = 8;
    writeVarUInt(total_rows, ostr);
    size_t bytes_size = (bits_per_row * total_rows + atom_size - 1) / atom_size;
    for (const auto & bloom_filter : bloom_filters)
        ostr.write(reinterpret_cast<const char *>(bloom_filter->getFilter().data()), bytes_size);
}

void MergeTreeIndexGranuleBloomFilter::fillingBloomFilter(BloomFilterPtr & bf, const Block & granule_index_block, size_t index_hash_column) const
{
    const auto & column = granule_index_block.getByPosition(index_hash_column);

    if (const auto * hash_column = typeid_cast<const ColumnUInt64 *>(column.column.get()))
    {
        const auto & hash_column_vec = hash_column->getData();

        for (const auto & bf_base_hash : hash_column_vec)
            for (size_t i = 0; i < hash_functions; ++i)
                bf->addHashWithSeed(bf_base_hash, BloomFilterHash::bf_hash_seed[i]);
    }
}

}
