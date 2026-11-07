#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>

#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <DataTypes/TimeSeries/TimeSeriesColumnNames.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{
    using Element = TimeSeriesHistogramsColumn;
    using Registry = TimeSeriesHistogramsColumns;

    [[noreturn]] void throwUnexpectedColumn(std::string_view column_name, std::string_view expected_type, const IColumn & column)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column `{}` of histogram samples must have type {}, got {}",
            column_name, expected_type, column.getName());
    }

    /// The column of an element of `source`, checked against the registry's type (`structureEquals` compares the column classes
    /// recursively, arrays with their data and tuples element by element) and cast to the class of the view member it initializes:
    /// `Ref` is `decltype(member)`, e.g. `const ColumnFloat64 &` or `ColumnArray &`. Throws ILLEGAL_COLUMN naming the element.
    template <typename Ref, typename Source>
    Ref checkedElement(const Source & source, Element element)
    {
        auto & column = source.element(element);
        if (!column.structureEquals(Registry::getEmptyColumn(element)))
            throwUnexpectedColumn(Registry::getName(element), Registry::getType(element), column);
        return assert_cast<Ref>(column);
    }

    /// The sources of the columns a view is built over: `timestamps`, the payload elements, and the offsets of the outer arrays.

    /// A tuple of the stored shape; `Tuple` is `const ColumnTuple` or `ColumnTuple`.
    template <typename Tuple>
    struct TupleSource
    {
        Tuple & tuple;

        explicit TupleSource(Tuple & tuple_) : tuple(tuple_)
        {
            const size_t expected_size = Registry::getAll().size() + 1;
            if (tuple.tupleSize() != expected_size)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "A tuple of histogram samples must have {} elements, got {}: {}",
                    expected_size, tuple.tupleSize(), tuple.getName());
        }

        auto & timestamps() const { return tuple.getColumn(Registry::getTimestampPosition()); }
        auto & element(Element element) const { return tuple.getColumn(Registry::getTuplePosition(element)); }
        static const ColumnArray::Offsets * outerOffsets() { return nullptr; }
    };

    /// The outer `histograms.*` columns of a block, all arrays with the offsets of `histograms.timestamp`.
    struct BlockSource
    {
        const Block & block;
        const ColumnArray & timestamp_array;

        explicit BlockSource(const Block & block_) : block(block_), timestamp_array(getOuterArray(TimeSeriesColumnNames::Timestamp)) { }

        const IColumn & timestamps() const { return timestamp_array.getData(); }

        const IColumn & element(Element element) const
        {
            const auto & array = getOuterArray(Registry::getName(element));
            if (!array.hasEqualOffsets(timestamp_array))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column `{}` of histogram samples has other array sizes than column `{}`",
                    Registry::getOuterColumnName(Registry::getName(element)), Registry::getOuterColumnName(TimeSeriesColumnNames::Timestamp));
            return array.getData();
        }

        const ColumnArray::Offsets * outerOffsets() const { return &timestamp_array.getOffsets(); }

    private:
        const ColumnArray & getOuterArray(std::string_view element_name) const
        {
            const auto outer_name = Registry::getOuterColumnName(element_name);
            const auto & column = *block.getByName(outer_name).column;
            const auto * array = typeid_cast<const ColumnArray *>(&column);
            if (!array)
                throwUnexpectedColumn(outer_name, "Array", column);
            return *array;
        }
    };

    /// The columns being built by TimeSeriesHistogramsColumnsMutableView's users.
    struct BuiltColumnsSource
    {
        IColumn & timestamps_column;
        const TimeSeriesHistogramsColumnsMutableView::Columns & payload;

        IColumn & timestamps() const { return timestamps_column; }
        IColumn & element(Element element) const { return *payload[static_cast<size_t>(element)]; }
    };

    /// The pairs of row `row` of an array of `Tuple(index Int32, count Count)`.
    template <typename Count>
    TimeSeriesHistogramsColumnsView::Pairs<Count> pairsAt(const ColumnArray & pairs, size_t row)
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(pairs.getData());
        const auto & indexes = assert_cast<const ColumnInt32 &>(tuple.getColumn(Registry::getPairIndexPosition())).getData();
        const auto & counts = assert_cast<const ColumnVector<Count> &>(tuple.getColumn(Registry::getPairCountPosition())).getData();
        const size_t start = pairs.getOffset(row);
        const size_t size = pairs.getSize(row);
        return {{indexes.data() + start, size}, {counts.data() + start, size}};
    }
}


template <typename Source>
TimeSeriesHistogramsColumnsView::TimeSeriesHistogramsColumnsView(const Source & source)
    : timestamps(source.timestamps())
    , flags(checkedElement<decltype(flags)>(source, Element::Flags))
    , counter_reset_hint(checkedElement<decltype(counter_reset_hint)>(source, Element::CounterResetHint))
    , schema(checkedElement<decltype(schema)>(source, Element::Schema))
    , zero_threshold(checkedElement<decltype(zero_threshold)>(source, Element::ZeroThreshold))
    , sum(checkedElement<decltype(sum)>(source, Element::Sum))
    , count_int(checkedElement<decltype(count_int)>(source, Element::CountInt))
    , zero_count_int(checkedElement<decltype(zero_count_int)>(source, Element::ZeroCountInt))
    , count_float(checkedElement<decltype(count_float)>(source, Element::CountFloat))
    , zero_count_float(checkedElement<decltype(zero_count_float)>(source, Element::ZeroCountFloat))
    , positive_int(checkedElement<decltype(positive_int)>(source, Element::PositiveInt))
    , negative_int(checkedElement<decltype(negative_int)>(source, Element::NegativeInt))
    , positive_float(checkedElement<decltype(positive_float)>(source, Element::PositiveFloat))
    , negative_float(checkedElement<decltype(negative_float)>(source, Element::NegativeFloat))
    , custom_values(checkedElement<decltype(custom_values)>(source, Element::CustomValues))
    , outer_offsets(source.outerOffsets())
{
}

TimeSeriesHistogramsColumnsView::TimeSeriesHistogramsColumnsView(const ColumnTuple & tuple)
    : TimeSeriesHistogramsColumnsView(TupleSource<const ColumnTuple>{tuple})
{
}

TimeSeriesHistogramsColumnsView::TimeSeriesHistogramsColumnsView(const Block & block)
    : TimeSeriesHistogramsColumnsView(BlockSource{block})
{
}

TimeSeriesHistogramsColumnsView::StoredColumns TimeSeriesHistogramsColumnsView::getColumnsInStoredOrder() const
{
    const auto element = [this](Element element_) -> const IColumn &
    {
        switch (element_)
        {
            case Element::Flags: return flags;
            case Element::CounterResetHint: return counter_reset_hint;
            case Element::Schema: return schema;
            case Element::ZeroThreshold: return zero_threshold;
            case Element::Sum: return sum;
            case Element::CountInt: return count_int;
            case Element::ZeroCountInt: return zero_count_int;
            case Element::CountFloat: return count_float;
            case Element::ZeroCountFloat: return zero_count_float;
            case Element::PositiveInt: return positive_int;
            case Element::NegativeInt: return negative_int;
            case Element::PositiveFloat: return positive_float;
            case Element::NegativeFloat: return negative_float;
            case Element::CustomValues: return custom_values;
        }
    };

    StoredColumns result;
    result[Registry::getTimestampPosition()] = &timestamps;
    for (const auto payload_element : Registry::getAll())
        result[Registry::getTuplePosition(payload_element)] = &element(payload_element);
    return result;
}

TimeSeriesHistogramsColumnsView::IntPairs TimeSeriesHistogramsColumnsView::intPairsAt(const ColumnArray & pairs, size_t row)
{
    return pairsAt<Int64>(pairs, row);
}

TimeSeriesHistogramsColumnsView::FloatPairs TimeSeriesHistogramsColumnsView::floatPairsAt(const ColumnArray & pairs, size_t row)
{
    return pairsAt<Float64>(pairs, row);
}

std::span<const Float64> TimeSeriesHistogramsColumnsView::floatsAt(const ColumnArray & values, size_t row)
{
    const auto & data = assert_cast<const ColumnFloat64 &>(values.getData()).getData();
    return {data.data() + values.getOffset(row), values.getSize(row)};
}


TimeSeriesHistogramsColumnsMutableView::Columns TimeSeriesHistogramsColumnsMutableView::createPayloadColumns()
{
    Columns result;
    for (const auto element : Registry::getAll())
        result[static_cast<size_t>(element)] = Registry::getDataType(element)->createColumn();
    return result;
}

template <typename Source>
TimeSeriesHistogramsColumnsMutableView::TimeSeriesHistogramsColumnsMutableView(const Source & source)
    : timestamps(source.timestamps())
    , flags(checkedElement<decltype(flags)>(source, Element::Flags))
    , counter_reset_hint(checkedElement<decltype(counter_reset_hint)>(source, Element::CounterResetHint))
    , schema(checkedElement<decltype(schema)>(source, Element::Schema))
    , zero_threshold(checkedElement<decltype(zero_threshold)>(source, Element::ZeroThreshold))
    , sum(checkedElement<decltype(sum)>(source, Element::Sum))
    , count_int(checkedElement<decltype(count_int)>(source, Element::CountInt))
    , zero_count_int(checkedElement<decltype(zero_count_int)>(source, Element::ZeroCountInt))
    , count_float(checkedElement<decltype(count_float)>(source, Element::CountFloat))
    , zero_count_float(checkedElement<decltype(zero_count_float)>(source, Element::ZeroCountFloat))
    , positive_int(checkedElement<decltype(positive_int)>(source, Element::PositiveInt))
    , negative_int(checkedElement<decltype(negative_int)>(source, Element::NegativeInt))
    , positive_float(checkedElement<decltype(positive_float)>(source, Element::PositiveFloat))
    , negative_float(checkedElement<decltype(negative_float)>(source, Element::NegativeFloat))
    , custom_values(checkedElement<decltype(custom_values)>(source, Element::CustomValues))
{
}

TimeSeriesHistogramsColumnsMutableView::TimeSeriesHistogramsColumnsMutableView(IColumn & timestamps_, const Columns & payload)
    : TimeSeriesHistogramsColumnsMutableView(BuiltColumnsSource{timestamps_, payload})
{
}

TimeSeriesHistogramsColumnsMutableView::TimeSeriesHistogramsColumnsMutableView(ColumnTuple & tuple)
    : TimeSeriesHistogramsColumnsMutableView(TupleSource<ColumnTuple>{tuple})
{
}

}
