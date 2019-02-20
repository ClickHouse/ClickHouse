#include <Storages/MergeTree/MergeTreeBloomFilterIndex.h>

#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}


MergeTreeBloomFilterIndexGranule::MergeTreeBloomFilterIndexGranule(const MergeTreeBloomFilterIndex & index)
    : IMergeTreeIndexGranule(), index(index), bloom_filter(index.bloom_filter_size, index.bloom_filter_hashes, index.seed)
{
}

void MergeTreeBloomFilterIndexGranule::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(
                "Attempt to write empty minmax index `" + index.name + "`", ErrorCodes::LOGICAL_ERROR);

    const auto & filter = bloom_filter.getFilter();
    ostr.write(reinterpret_cast<const char *>(filter.data()), index.bloom_filter_size);
}

void MergeTreeBloomFilterIndexGranule::deserializeBinary(ReadBuffer & istr)
{
    std::vector<UInt8> filter(index.bloom_filter_size, 0);
    istr.read(reinterpret_cast<char *>(filter.data()), index.bloom_filter_size);
    bloom_filter.setFilter(std::move(filter));
}

void MergeTreeBloomFilterIndexGranule::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

    const auto & column = block.getByName(index.columns.front()).column;
    for (size_t i = 0; i < rows_read; ++i)
    {
        auto ref = column->getDataAt(*pos + i);
        size_t cur = 0;
        size_t token_start = 0;
        size_t token_len = 0;
        while (cur < ref.size && index.tokenExtractorFunc(ref.data, ref.size, cur, token_start, token_len))
        {
            bloom_filter.add(ref.data + token_start, token_len);
            cur = token_start + token_len;
        }
    }

    *pos += rows_read;
}

}
