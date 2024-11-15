#include <memory>
#include <Storages/MergeTree/MergeTreePrimaryIndexColumn.h>
#include <Storages/MergeTree/MergeTreePrimaryIndexNumeric.h>
#include <Columns/ColumnLowCardinality.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IndexColumnCommon::IndexColumnCommon(IndexBlocks blocks_, size_t block_size_)
    : blocks(std::move(blocks_)), block_size(block_size_)
{
}

size_t IndexColumnCommon::bytes() const
{
    size_t res = 0;
    for (const auto & block : blocks)
        res += block->bytes();
    return res;
}

size_t IndexColumnCommon::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & block : blocks)
        res += block->allocatedBytes();
    return res;
}

void IndexColumnCommon::get(size_t n, Field & field) const
{
    const auto & block = blocks[n / block_size];
    block->get(n % block_size, field);
}

bool IndexColumnCommon::isCompressed() const
{
    return blocks.size() > 1 || (blocks.size() == 1 && !dynamic_cast<IndexBlockRaw *>(blocks.front().get()));
}

ColumnPtr IndexColumnCommon::getRawColumn() const
{
    if (isCompressed())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get raw column because index is compressed");

    return dynamic_cast<IndexBlockRaw *>(blocks.front().get())->column;
}

IndexColumnLowCardinality::IndexColumnLowCardinality(IndexColumnPtr index_, ColumnPtr dictionary_)
    : index(std::move(index_)), dictionary(std::move(dictionary_))
{
}

size_t IndexColumnLowCardinality::bytes() const
{
    return index->bytes() + dictionary->byteSize();
}

size_t IndexColumnLowCardinality::allocatedBytes() const
{
     return index->allocatedBytes() + dictionary->allocatedBytes();
}

bool IndexColumnLowCardinality::isCompressed() const
{
    return index->isCompressed();
}

void IndexColumnLowCardinality::get(size_t n, Field & field) const
{
    Field pos;
    index->get(n, pos);
    dictionary->get(pos.safeGet<UInt64>(), field);
}

ColumnPtr IndexColumnLowCardinality::getRawColumn() const
{
    if (isCompressed())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get raw column because index is compressed");

    return ColumnLowCardinality::create(dictionary, index->getRawColumn());
}

IndexColumnPtr createIndexColumnRaw(ColumnPtr column)
{
    IndexBlocks blocks;
    size_t num_rows = column->size();

    blocks.push_back(std::make_unique<IndexBlockRaw>(std::move(column)));
    return std::make_unique<IndexColumnCommon>(std::move(blocks), num_rows);
}

IndexColumnPtr createIndexColumnLowCardinality(ColumnPtr column, size_t block_size, double max_ratio_to_compress)
{
    const auto * column_lc = typeid_cast<const ColumnLowCardinality *>(column.get());
    if (!column_lc)
        return nullptr;

    auto dictionary = column_lc->getDictionaryPtr();
    auto index = createIndexColumnNumeric(column_lc->getIndexesPtr(), block_size, max_ratio_to_compress);

    if (!index)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create index for positions of LowCardinaliry column. It's a bug");

    return std::make_unique<IndexColumnLowCardinality>(std::move(index), std::move(dictionary));
}

}
