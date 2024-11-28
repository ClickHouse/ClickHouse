#include <Storages/MergeTree/MergeTreePrimaryIndexNumeric.h>
#include <Storages/MergeTree/MergeTreePrimaryIndexColumn.h>
#include <base/TypeList.h>
#include <Columns/ColumnVector.h>

namespace DB
{

namespace
{

constexpr size_t divRoundUp(size_t value, size_t dividend)
{
    return (value + dividend - 1) / dividend;
}

constexpr size_t getNumberOfElementsForBits(size_t num_bits)
{
    return divRoundUp(num_bits, 64) + 1;
}

constexpr bool needCompress(size_t compressed, size_t uncompressed, double max_ratio)
{
    return static_cast<double>(compressed) / uncompressed <= max_ratio;
}

constexpr size_t getNumBytesForRLEOffset(size_t n)
{
    if (n <= std::numeric_limits<UInt8>::max()) return sizeof(UInt8);
    if (n <= std::numeric_limits<UInt16>::max()) return sizeof(UInt16);
    if (n <= std::numeric_limits<UInt32>::max()) return sizeof(UInt32);

    return sizeof(UInt64);
}

template <typename Offset, typename Column>
IndexBlockPtr buildRLEBlock(const Column & column, size_t from, size_t to)
{
    auto block = std::make_unique<IndexBlockRLE<Offset>>();
    auto block_column = column.cloneEmpty();

    const auto & data = column.getData();
    auto & res_data = assert_cast<Column &>(*block_column).getData();

    res_data.push_back(data[from]);
    block->offsets.push_back(0);

    for (size_t j = from + 1; j < to; ++j)
    {
        if (data[j] != data[j - 1])
        {
            res_data.push_back(data[j]);
            block->offsets.push_back(static_cast<Offset>(j - from));
        }
    }

    block->column = std::move(block_column);
    return block;
}

template <typename Column>
IndexBlockPtr buildRLEBlock(size_t rle_offset_bytes, const Column & column, size_t from, size_t to)
{
    switch (rle_offset_bytes)
    {
        case sizeof(UInt8):
            return buildRLEBlock<UInt8>(column, from, to);
        case sizeof(UInt16):
            return buildRLEBlock<UInt16>(column, from, to);
        case sizeof(UInt32):
            return buildRLEBlock<UInt32>(column, from, to);
        default:
            return buildRLEBlock<UInt64>(column, from, to);
    }
}

template <typename U>
void buildDelta(PaddedPODArray<UInt64> & delta, UInt8 bit_size, U base, const PaddedPODArray<U> & data, size_t from, size_t to)
{
    size_t total_bits = (to - from) * bit_size;
    size_t total_elements = getNumberOfElementsForBits(total_bits);

    delta.resize_fill(total_elements);
    size_t bit_offset = 0;

    for (size_t i = from; i < to; ++i)
    {
        U delta_elem = data[i] - base;
        chassert((delta_elem & maskLowBits<UInt64>(bit_size)) == static_cast<UInt64>(delta_elem));

        writeBitsPacked(delta.begin(), bit_offset, delta_elem);
        bit_offset += bit_size;
    }
}

template <typename U>
IndexBlockPtr buildDeltaBlock(UInt8 bit_size, U base, const PaddedPODArray<U> & data, size_t from, size_t to)
{
    auto block = std::make_unique<IndexBlockDelta<U>>();

    block->base = base;
    block->bit_size = bit_size;
    buildDelta(block->delta, block->bit_size, base, data, from, to);

    return block;
}

template <typename U>
struct ChosenCompression
{
    IIndexBlock::Type type = IIndexBlock::Type::Raw;

    /// Potential size of compressed block.
    UInt64 compressed_size = 0;

    /// Number of bytes required for offsets for RLE compression.
    UInt8 rle_offset_bytes = 0;

    /// The minimal element in block that can be used for Delta compression.
    U delta_base = 0;

    /// Number of bits required to store max delta in block for Delta compression.
    UInt8 delta_bit_size = 0;
};

template <typename U>
ChosenCompression<U> chooseCompression(const PaddedPODArray<U> & data, size_t from, size_t num_rows, double max_ratio)
{
    using Type = IIndexBlock::Type;

    U min_value = data[from];
    U max_value = data[from];
    size_t num_groups = 1;

    for (size_t j = from; j < from + num_rows; ++j)
    {
        min_value = std::min(min_value, data[j]);
        max_value = std::max(max_value, data[j]);

        if (j != from && data[j] != data[j - 1])
            ++num_groups;
    }

    ChosenCompression<U> chosen;
    size_t uncompressed_bytes = sizeof(U) * num_rows;

    /// Use minimal number of bytes to store offsets. Max offset can be `num_rows - 1`.
    size_t rle_offset_bytes = getNumBytesForRLEOffset(num_rows);
    size_t rle_encoding_bytes = (sizeof(U) + rle_offset_bytes) * num_groups;

    UInt64 diff = max_value - min_value;
    size_t delta_bit_size = diff ? bitScanReverse(diff) + 1 : 0;
    size_t delta_enconding_bytes = sizeof(U) + sizeof(UInt8) + getNumberOfElementsForBits(delta_bit_size * num_rows) * sizeof(UInt64);

    if (diff == 0)
    {
        chosen.type = Type::Const;
        chosen.compressed_size = sizeof(U);
    }
    else if (rle_encoding_bytes < delta_enconding_bytes)
    {
        chosen.type = Type::RLE;
        chosen.compressed_size = rle_encoding_bytes;
        chosen.rle_offset_bytes = rle_offset_bytes;
    }
    else
    {
        chosen.type = Type::Delta;
        chosen.compressed_size = delta_enconding_bytes;
        chosen.delta_base = min_value;
        chosen.delta_bit_size = delta_bit_size;
    }

    if (!needCompress(chosen.compressed_size, uncompressed_bytes, max_ratio))
    {
        chosen = {};
        chosen.compressed_size = uncompressed_bytes;
    }

    return chosen;
}

template <typename Column>
IndexBlocks buildBlocks(const Column & column, size_t block_size, double max_ratio_to_compress)
{
    using U = Column::ValueType;
    const auto & data = column.getData();

    size_t column_size = column.size();
    size_t total_compressed_bytes = 0;
    size_t total_bytes = column.byteSize();

    std::vector<ChosenCompression<U>> compressions;
    compressions.reserve(column_size / block_size);

    for (size_t i = 0; i < column_size; i += block_size)
    {
        size_t num_rows = std::min(block_size, column_size - i);
        auto & chosen = compressions.emplace_back(chooseCompression(data, i, num_rows, max_ratio_to_compress));
        total_compressed_bytes += chosen.compressed_size;
    }

    IndexBlocks blocks;

    if (!needCompress(total_compressed_bytes, total_bytes, max_ratio_to_compress))
    {
        blocks.push_back(std::make_unique<IndexBlockRaw>(column.getPtr()));
        return blocks;
    }

    using Type = IIndexBlock::Type;
    blocks.reserve(compressions.size());

    for (size_t i = 0; i < compressions.size(); ++i)
    {
        size_t from = i * block_size;
        size_t to = std::min(column_size, from + block_size);
        IndexBlockPtr block;

        if (compressions[i].type == Type::Const)
        {
            block = std::make_unique<IndexBlockConstNumeric<U>>(data[from]);
        }
        else if (compressions[i].type == Type::Delta)
        {
            block = buildDeltaBlock(compressions[i].delta_bit_size, static_cast<U>(compressions[i].delta_base), data, from, to);
        }
        else if (compressions[i].type == Type::RLE)
        {
            block = buildRLEBlock(compressions[i].rle_offset_bytes, column, from, to);
        }
        else
        {
            auto raw_column = column.cloneEmpty();
            raw_column->insertRangeFrom(column, from, to - from);
            block = std::make_unique<IndexBlockRaw>(std::move(raw_column));
        }

        blocks.push_back(std::move(block));
    }

    return blocks;
}

}

IndexColumnPtr createIndexColumnNumeric(ColumnPtr column, size_t block_size, double max_ratio_to_compress)
{
    IndexBlocks blocks;

    using TypeListArgs = TypeList<Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64>;

    TypeListUtils::forEach(TypeListArgs{}, [&]<typename T>(TypeList<T>)
    {
        if (const auto * column_typed = checkAndGetColumn<ColumnVector<T>>(column.get()))
            blocks = buildBlocks(*column_typed, block_size, max_ratio_to_compress);
    });

    if (blocks.empty())
        return nullptr;

    if (blocks.size() == 1)
        block_size = column->size();

    return std::make_unique<IndexColumnCommon>(std::move(blocks), block_size);
}

}
