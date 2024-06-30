#include <algorithm>
#include <limits>
#include <Storages/MergeTree/MergeTreePrimaryIndex.h>
#include "Common/Logger.h"
#include "Common/logger_useful.h"
#include "Common/typeid_cast.h"
#include "base/types.h"
#include <base/TypeList.h>
#include <DataTypes/IDataType.h>
#include <Columns/ColumnVector.h>

namespace DB
{

namespace
{

bool needCompress(size_t compressed, size_t uncompressed, double max_ratio)
{
    return static_cast<double>(compressed) / uncompressed <= max_ratio;
}

constexpr size_t divRoundUp(size_t value, size_t dividend)
{
    return (value + dividend - 1) / dividend;
}

constexpr size_t getNumberOfElementsForBits(size_t num_bits)
{
    return divRoundUp(num_bits, 64) + 1;
}

constexpr size_t getNumBytesForRLEOffset(size_t block_size)
{
    if (block_size <= std::numeric_limits<UInt8>::max())
        return sizeof(UInt8);

    chassert(block_size <= std::numeric_limits<UInt16>::max());
    return sizeof(UInt16);
}

template <typename Column>
void buildDelta(PaddedPODArray<UInt64> & delta, UInt8 bit_size, const Column & column, size_t from, size_t to)
{
    size_t total_bits = (to - from) * bit_size;
    size_t total_elements = getNumberOfElementsForBits(total_bits);

    const auto & data = column.getData();

    delta.resize_fill(total_elements);
    size_t bit_offset = 0;

    for (size_t i = from; i < to; ++i)
    {
        UInt64 delta_elem = data[i] - data[from];
        chassert((delta_elem & maskLowBits<UInt64>(bit_size)) == delta_elem);

        writeBitsPacked(delta.begin(), bit_offset, delta_elem);
        bit_offset += bit_size;
    }
}

template <typename Offset, typename Column>
RLEBlock<Offset> buildRLEBlock(const Column & column, size_t from, size_t to)
{
    RLEBlock<Offset> block;
    auto block_column = column.cloneEmpty();

    const auto & data = column.getData();
    auto & res_data = assert_cast<Column &>(*block_column).getData();

    res_data.push_back(data[from]);
    block.offsets.push_back(0);

    for (size_t j = from + 1; j < to; ++j)
    {
        if (data[j] != data[j - 1])
        {
            res_data.push_back(data[j]);
            block.offsets.push_back(j - from);
        }
    }

    block.column = std::move(block_column);
    return block;
}

template <typename Block, typename T, typename Column>
std::vector<Block> buildBlocks(const Column & column, size_t block_size, double max_ratio_to_compress)
{
    using U = Column::ValueType;

    size_t column_size = column.size();
    const auto & data = column.getData();

    enum Compression : UInt8
    {
        None,
        Const,
        Delta,
        RLE,
    };

    std::vector<UInt8> delta_bit_sizes;
    std::vector<Compression> compressions;

    delta_bit_sizes.reserve(column_size / block_size);
    compressions.reserve(column_size / block_size);

    size_t total_compressed_bytes = 0;
    size_t total_bytes = column.byteSize();
    size_t bytes_for_rle_offset = getNumBytesForRLEOffset(block_size);

    for (size_t i = 0; i < column_size; i += block_size)
    {
        size_t len = std::min(block_size, column_size - i);

        U min_value = data[i];
        U max_value = data[i];
        size_t num_groups = 1;

        for (size_t j = i + 1; j < i + len; ++j)
        {
            min_value = std::min(min_value, data[j]);
            max_value = std::max(max_value, data[j]);

            if (data[j] != data[j - 1])
                ++num_groups;
        }

        UInt64 diff = max_value - min_value;
        size_t delta_bit_size = diff == 0 ? 0 : bitScanReverse(diff) + 1;

        size_t rle_encoding_bytes = (sizeof(U) + bytes_for_rle_offset) * num_groups;
        size_t delta_enconding_bytes = sizeof(T) + sizeof(UInt8) + getNumberOfElementsForBits(delta_bit_size * len) * 8;

        Compression compression = Compression::None;

        size_t uncompressed_bytes = sizeof(U) * len;
        size_t compressed_bytes = uncompressed_bytes;

        if (diff == 0)
        {
            compression = Compression::Const;
            compressed_bytes = sizeof(T);
        }
        else if (rle_encoding_bytes < delta_enconding_bytes)
        {
            compression = Compression::RLE;
            compressed_bytes = rle_encoding_bytes;
        }
        else
        {
            compression = Compression::Delta;
            compressed_bytes = delta_enconding_bytes;
        }

        if (!needCompress(compressed_bytes, uncompressed_bytes, max_ratio_to_compress))
        {
            compressed_bytes = uncompressed_bytes;
            compression = Compression::None;
            delta_bit_size = sizeof(U);
        }

        compressions.push_back(compression);
        delta_bit_sizes.push_back(delta_bit_size);
        total_compressed_bytes += compressed_bytes;
    }

    std::vector<Block> blocks;

    if (!needCompress(total_compressed_bytes, total_bytes, max_ratio_to_compress))
    {
        blocks.push_back(RawBlock{column.getPtr()});
        return blocks;
    }

    blocks.reserve(compressions.size());

    for (size_t i = 0; i < compressions.size(); ++i)
    {
        size_t from = i * block_size;
        size_t to = std::min(column_size, from + block_size);

        if (compressions[i] == Compression::Const)
        {
            blocks.push_back(ConstBlock<T>{static_cast<T>(data[from])});
        }
        else if (compressions[i] == Compression::Delta)
        {
            DeltaBlock<T> block;

            block.base = static_cast<T>(data[from]);
            block.bit_size = delta_bit_sizes[i];
            buildDelta(block.delta, block.bit_size, column, from, to);

            blocks.push_back(std::move(block));
        }
        else if (compressions[i] == Compression::RLE)
        {
            if (bytes_for_rle_offset == sizeof(UInt8))
                blocks.push_back(buildRLEBlock<UInt8>(column, from, to));
            else
                blocks.push_back(buildRLEBlock<UInt16>(column, from, to));
        }
        else
        {
            auto block_column = column.cloneEmpty();
            block_column->insertRangeFrom(column, from, to - from);
            blocks.push_back(RawBlock{std::move(block_column)});
        }
    }

    return blocks;
}

}

PrimaryIndex::PrimaryIndex(Columns columns_, const Settings & settings)
    : num_rows(columns_.empty() ? 0 : columns_[0]->size())
{
    for (auto & column : columns_)
    {
        if (!settings.compress)
        {
            columns.push_back(std::make_unique<IndexColumnGeneric>(std::move(column)));
            continue;
        }

        WhichDataType which(column->getDataType());

        if (which.isNativeInt())
        {
            columns.push_back(std::make_unique<IndexColumnInt<true>>(std::move(column), settings));
        }
        else if (which.isNativeUInt())
        {
            columns.push_back(std::make_unique<IndexColumnInt<false>>(std::move(column), settings));
        }
        else
        {
            columns.push_back(std::make_unique<IndexColumnGeneric>(std::move(column)));
        }
    }
}

size_t PrimaryIndex::bytes() const
{
    size_t result = 0;
    for (const auto & column : columns)
        result += column->bytes();
    return result;
}

size_t PrimaryIndex::allocatedBytes() const
{
    size_t result = 0;
    for (const auto & column : columns)
        result += column->allocatedBytes();
    return result;
}

bool PrimaryIndex::isCompressed() const
{
    return std::ranges::any_of(columns, [](const auto & column) { return column->isCompressed(); });
}

Columns PrimaryIndex::getRawColumns() const
{
    Columns result;
    result.reserve(columns.size());

    for (const auto & column : columns)
        result.push_back(column->getRawColumn());
    return result;
}

void PrimaryIndex::get(size_t col_idx, size_t row_idx, Field & field) const
{
    return columns.at(col_idx)->get(row_idx, field);
}

Field PrimaryIndex::get(size_t col_idx, size_t row_idx) const
{
    Field res;
    get(col_idx, row_idx, res);
    return res;
}

template <bool is_signed>
IndexColumnInt<is_signed>::IndexColumnInt(ColumnPtr column, const PrimaryIndex::Settings & settings)
{
    using TypeListArgs = std::conditional_t<is_signed,
        TypeList<Int8, Int16, Int32, Int64>,
        TypeList<UInt8, UInt16, UInt32, UInt64>>;

    TypeListUtils::forEach(TypeListArgs{}, [&]<typename U>(TypeList<U>)
    {
        if (const auto * column_typed = checkAndGetColumn<ColumnVector<U>>(column.get()))
            blocks = buildBlocks<Block, T>(*column_typed, settings.block_size, settings.max_ratio_to_compress);
    });

    if (blocks.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Got invalid column {}. Expected {} integer column",
            column->getName(), is_signed ? "signed" : "unsigned");

    block_size = blocks.size() == 1 ? column->size() : settings.block_size;
}

template <bool is_signed>
size_t IndexColumnInt<is_signed>::bytes() const
{
    size_t result = 0;
    for (const auto & block : blocks)
        std::visit([&](const auto & arg) { result += arg.bytes(); }, block);
    return result;
}

template <bool is_signed>
size_t IndexColumnInt<is_signed>::allocatedBytes() const
{
    size_t result = 0;
    for (const auto & block : blocks)
        std::visit([&](const auto & arg) { result += arg.allocatedBytes(); }, block);
    return result;
}

template <bool is_signed>
void IndexColumnInt<is_signed>::get(size_t n, Field & field) const
{
    const auto & block = blocks[n / block_size];
    std::visit([&] (const auto & arg) { arg.get(n % block_size, field); }, block);
}

template <bool is_signed>
bool IndexColumnInt<is_signed>::isCompressed() const
{
    return blocks.size() == 1 && !std::holds_alternative<RawBlock>(blocks.front());
}

template <bool is_signed>
ColumnPtr IndexColumnInt<is_signed>::getRawColumn() const
{
    if (isCompressed())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get raw column because index is compressed");

    const auto * raw_block = std::get_if<RawBlock>(&blocks.front());
    return raw_block->column;
}

template class IndexColumnInt<true>;
template class IndexColumnInt<false>;

}
