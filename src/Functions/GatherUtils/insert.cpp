#include <Common/Exception.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Selectors.h>

namespace DB::ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace DB::GatherUtils
{

namespace
{

template <typename Position>
[[noreturn]] [[gnu::cold]] NO_INLINE void throwInsertPositionOutOfBounds(Position position, size_t array_size)
{
    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "Array insertion position {} is out of bounds for an array of size {}", position, array_size);
}

size_t normalizeInsertPosition(UInt64 position, size_t array_size)
{
    /// Compare the zero-based position to avoid overflowing array_size + 1.
    if (unlikely(position == 0 || position - 1 > array_size))
        throwInsertPositionOutOfBounds(position, array_size);

    return static_cast<size_t>(position - 1);
}

size_t normalizeInsertPosition(Int64 position, size_t array_size)
{
    if (position >= 0)
        return normalizeInsertPosition(static_cast<UInt64>(position), array_size);

    /// -(position + 1) is safe even for Int64's minimum value.
    const UInt64 from_end = static_cast<UInt64>(-(position + 1));
    if (unlikely(from_end > array_size))
        throwInsertPositionOutOfBounds(position, array_size);

    return array_size - static_cast<size_t>(from_end);
}

template <typename Source, typename ValueSource, typename Sink, typename GetPosition>
void NO_INLINE insertImpl(Source && array_source, ValueSource && value_source, Sink && sink, GetPosition get_position)
{
    sink.reserve(array_source.getSizeForReserve() + value_source.getSizeForReserve());

    while (!array_source.isEnd())
    {
        const size_t insert_position = normalizeInsertPosition(get_position(array_source.rowNum()), array_source.getElementSize());
        writeSlice(array_source.getSliceFromLeft(0, insert_position), sink);
        writeSlice(value_source.getWhole(), sink);
        writeSlice(array_source.getSliceFromLeft(insert_position), sink);

        sink.next();
        array_source.next();
        value_source.next();
    }
}

struct ArrayInsert : public ArrayAndValueSourceSelectorBySink<ArrayInsert>
{
    template <typename ArraySource, typename ValueSource, typename Sink, typename Position>
    static void selectArrayAndValueSourceBySink(ArraySource && array_source, ValueSource && value_source, Sink && sink, Position position)
    {
        insertImpl(array_source, value_source, sink, [position](size_t) { return position; });
    }

    template <typename ArraySource, typename ValueSource, typename Sink>
    static void selectArrayAndValueSourceBySink(
        ArraySource && array_source, ValueSource && value_source, Sink && sink, const IColumn & position_column, bool position_is_unsigned)
    {
        if (position_is_unsigned)
            insertImpl(array_source, value_source, sink, [&position_column](size_t row) { return position_column.getUInt(row); });
        else
            insertImpl(array_source, value_source, sink, [&position_column](size_t row) { return position_column.getInt(row); });
    }
};

}

void insertConstantPosition(IArraySource & array_source, IValueSource & value_source, IArraySink & sink, Int64 position)
{
    ArrayInsert::select(sink, array_source, value_source, position);
}

void insertConstantPosition(IArraySource & array_source, IValueSource & value_source, IArraySink & sink, UInt64 position)
{
    ArrayInsert::select(sink, array_source, value_source, position);
}

void insertDynamicPosition(
    IArraySource & array_source, IValueSource & value_source, IArraySink & sink, const IColumn & position_column, bool position_is_unsigned)
{
    ArrayInsert::select(sink, array_source, value_source, position_column, position_is_unsigned);
}

ColumnArray::MutablePtr insertWithLowCardinality(
    const ColumnArray & array_column,
    const IColumn & value_column,
    const IColumn & position_column,
    bool array_is_const,
    bool value_is_const,
    bool position_is_unsigned,
    size_t rows)
{
    auto result = ColumnArray::create(array_column.getData().cloneEmpty());
    auto & result_elements = result->getData();
    auto & result_offsets = result->getOffsets();
    result_offsets.reserve(rows);

    const auto & elements = array_column.getData();
    const auto & offsets = array_column.getOffsets();

    for (size_t row = 0; row < rows; ++row)
    {
        const size_t array_row = array_is_const ? 0 : row;
        const size_t start = array_row ? offsets[array_row - 1] : 0;
        const size_t array_size = array_column.getSize(array_row);
        const size_t insert_position = position_is_unsigned
            ? normalizeInsertPosition(position_column.getUInt(row), array_size)
            : normalizeInsertPosition(position_column.getInt(row), array_size);

        result_elements.insertRangeFrom(elements, start, insert_position);
        result_elements.insertFrom(value_column, value_is_const ? 0 : row);
        result_elements.insertRangeFrom(elements, start + insert_position, array_size - insert_position);
        result_offsets.push_back(result_elements.size());
    }

    return result;
}

}
