#include <Storages/MergeTree/MergeTreeIndexGranuleBloomFilter.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/HashTable/Hash.h>
#include <Interpreters/BloomFilterHash.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexGranuleBloomFilter::MergeTreeIndexGranuleBloomFilter(size_t bits_per_row_, size_t hash_functions_, size_t index_columns_)
    : bits_per_row(bits_per_row_), hash_functions(hash_functions_), bloom_filters(index_columns_)
{
    total_rows = 0;
    for (size_t column = 0; column < index_columns_; ++column)
        bloom_filters[column] = std::make_shared<BloomFilter>(bits_per_row, hash_functions, 0);
}

MergeTreeIndexGranuleBloomFilter::MergeTreeIndexGranuleBloomFilter(
    size_t bits_per_row_, size_t hash_functions_, const std::vector<HashSet<UInt64>>& column_hashes_)
        : bits_per_row(bits_per_row_), hash_functions(hash_functions_), bloom_filters(column_hashes_.size())
{
    if (column_hashes_.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule_index_blocks empty or total_rows is zero.");

    size_t bloom_filter_max_size = 0;
    for (const auto & column_hash : column_hashes_)
        bloom_filter_max_size = std::max(bloom_filter_max_size, column_hash.size());

    static size_t atom_size = 8;

    // If multiple columns are given, we will initialize all the bloom filters
    // with the size of the highest-cardinality one. This is done for compatibility with
    // existing binary serialization format
    total_rows = bloom_filter_max_size;
    size_t bytes_size = (bits_per_row * total_rows + atom_size - 1) / atom_size;

    for (size_t column = 0, columns = column_hashes_.size(); column < columns; ++column)
    {
        bloom_filters[column] = std::make_shared<BloomFilter>(bytes_size, hash_functions, 0);
        fillingBloomFilter(bloom_filters[column], column_hashes_[column]);
    }
}

bool MergeTreeIndexGranuleBloomFilter::empty() const
{
    return !total_rows;
}

void MergeTreeIndexGranuleBloomFilter::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    readVarUInt(total_rows, istr);

    static size_t atom_size = 8;
    size_t bytes_size = (bits_per_row * total_rows + atom_size - 1) / atom_size;
    size_t read_size = bytes_size;
    for (auto & filter : bloom_filters)
    {
        filter->resize(bytes_size);
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        read_size = filter->getFilter().size() * sizeof(BloomFilter::UnderType);
#endif
        istr.readStrict(reinterpret_cast<char *>(filter->getFilter().data()), read_size);
    }
}

void MergeTreeIndexGranuleBloomFilter::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty bloom filter index.");

    writeVarUInt(total_rows, ostr);

    static size_t atom_size = 8;
    size_t write_size = (bits_per_row * total_rows + atom_size - 1) / atom_size;
    for (const auto & bloom_filter : bloom_filters)
    {
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        write_size = bloom_filter->getFilter().size() * sizeof(BloomFilter::UnderType);
#endif
        ostr.write(reinterpret_cast<const char *>(bloom_filter->getFilter().data()), write_size);
    }
}

void MergeTreeIndexGranuleBloomFilter::fillingBloomFilter(BloomFilterPtr & bf, const HashSet<UInt64> &hashes) const
{
    for (const auto & bf_base_hash : hashes)
        for (size_t i = 0; i < hash_functions; ++i)
            bf->addHashWithSeed(bf_base_hash.getKey(), BloomFilterHash::bf_hash_seed[i]);
}

}
