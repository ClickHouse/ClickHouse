#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>

#include <Columns/ColumnsDateTime.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{
    using Element = TimeSeriesHistogramsColumn;
    namespace Registry = TimeSeriesHistogramsColumns;
    using PayloadColumns = TimeSeriesHistogramPayloadColumnsMutableView::Columns;

    [[noreturn]] void throwUnexpectedColumn(std::string_view column_name, std::string_view expected_type, const IColumn & column)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column `{}` of histogram samples must have type {}, got {}",
            column_name, expected_type, column.getName());
    }

    /// The column of `element` of `source` as `Ref`, the type of the view member; throws ILLEGAL_COLUMN if it isn't of the registry's type.
    template <typename Ref, typename Source>
    Ref checkedElement(const Source & source, Element element)
    {
        auto & column = source.element(element);
        /// `structureEquals` compares the column classes recursively, so one check covers the data of the arrays and the tuple elements.
        if (!column.structureEquals(Registry::getEmptyColumn(element)))
            throwUnexpectedColumn(Registry::getName(element), Registry::getType(element), column);
        return assert_cast<Ref>(column);
    }

    void checkTimestamps(const IColumn & column)
    {
        /// `ColumnDateTime` is `ColumnUInt32`, so the second check covers `DateTime` and `UInt32`.
        if (!typeid_cast<const ColumnDateTime64 *>(&column) && !typeid_cast<const ColumnUInt32 *>(&column))
            throwUnexpectedColumn(Registry::getTimestampName(), "DateTime64, DateTime or UInt32", column);
    }

    /// The sources of the columns a view is built over: the payload elements, the timestamps and the offsets of the outer array.

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

        [[nodiscard]] auto & timestamps() const { return tuple.getColumn(Registry::getTimestampPosition()); }
        [[nodiscard]] auto & element(Element element) const { return tuple.getColumn(Registry::getTuplePosition(element)); }
        static const ColumnArray::Offsets * outerOffsets() { return nullptr; }
    };

    template <typename Tuple>
    TupleSource(Tuple &) -> TupleSource<Tuple>;

    /// The tuples of an array of tuples of the stored shape.
    struct ArraySource
    {
        const ColumnArray & array;
        const TupleSource<const ColumnTuple> tuple;

        explicit ArraySource(const ColumnArray & array_) : array(array_), tuple(getTuple(array_)) { }

        [[nodiscard]] const IColumn & timestamps() const { return tuple.timestamps(); }
        [[nodiscard]] const IColumn & element(Element element) const { return tuple.element(element); }
        [[nodiscard]] const ColumnArray::Offsets * outerOffsets() const { return &array.getOffsets(); }

    private:
        static const ColumnTuple & getTuple(const ColumnArray & array)
        {
            const auto * tuple = typeid_cast<const ColumnTuple *>(&array.getData());
            if (!tuple)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "An array of histogram samples must have type Array(Tuple(...)), got {}",
                    array.getName());
            return *tuple;
        }
    };

    /// The columns being built by the users of the mutable views: the payload columns, and the timestamps if the view has them.
    struct BuiltColumnsSource
    {
        IColumn * timestamps_column = nullptr;
        const PayloadColumns & payload;

        [[nodiscard]] IColumn & timestamps() const { return *timestamps_column; }
        [[nodiscard]] IColumn & element(Element element) const { return *payload[static_cast<size_t>(element)]; }
        static const ColumnArray::Offsets * outerOffsets() { return nullptr; }
    };

    /// The columns of a mutable view, for its read-only counterpart; `timestamps` and `outerOffsets` only for a view with timestamps.
    template <typename View>
    struct ViewSource
    {
        const View & view;

        [[nodiscard]] IColumn & timestamps() const { return view.timestamps; }
        [[nodiscard]] IColumn & element(Element element) const { return view.getPayloadColumn(element); }
        [[nodiscard]] const ColumnArray::Offsets * outerOffsets() const { return view.outer_offsets; }
    };

    template <typename View>
    ViewSource(const View &) -> ViewSource<View>;

    /// The pairs of row `row` of an array of `Tuple(index Int32, count Count)`.
    template <typename Count>
    TimeSeriesHistogramBucketPairs<Count> pairsAt(const ColumnArray & pairs, size_t row)
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(pairs.getData());
        const auto & indexes = assert_cast<const ColumnInt32 &>(tuple.getColumn(Registry::getPairIndexPosition())).getData();
        const auto & counts = assert_cast<const ColumnVector<Count> &>(tuple.getColumn(Registry::getPairCountPosition())).getData();
        const size_t start = pairs.getOffset(row);
        const size_t size = pairs.getSize(row);
        return {{indexes.data() + start, size}, {counts.data() + start, size}};
    }
}


template <bool is_const>
template <typename Source>
TimeSeriesHistogramPayloadColumnsViewImpl<is_const>::TimeSeriesHistogramPayloadColumnsViewImpl(FromSource, const Source & source)
    : is_float(checkedElement<Ref<ColumnUInt8>>(source, Element::IsFloat))
    , counter_reset_hint(checkedElement<Ref<ColumnInt8>>(source, Element::CounterResetHint))
    , schema(checkedElement<Ref<ColumnInt8>>(source, Element::Schema))
    , zero_threshold(checkedElement<Ref<ColumnFloat64>>(source, Element::ZeroThreshold))
    , sum(checkedElement<Ref<ColumnFloat64>>(source, Element::Sum))
    , count_int(checkedElement<Ref<ColumnInt64>>(source, Element::CountInt))
    , zero_count_int(checkedElement<Ref<ColumnInt64>>(source, Element::ZeroCountInt))
    , count_float(checkedElement<Ref<ColumnFloat64>>(source, Element::CountFloat))
    , zero_count_float(checkedElement<Ref<ColumnFloat64>>(source, Element::ZeroCountFloat))
    , positive_int(checkedElement<Ref<ColumnArray>>(source, Element::PositiveInt))
    , negative_int(checkedElement<Ref<ColumnArray>>(source, Element::NegativeInt))
    , positive_float(checkedElement<Ref<ColumnArray>>(source, Element::PositiveFloat))
    , negative_float(checkedElement<Ref<ColumnArray>>(source, Element::NegativeFloat))
    , custom_values(checkedElement<Ref<ColumnArray>>(source, Element::CustomValues))
{
}

template <bool is_const>
TimeSeriesHistogramPayloadColumnsViewImpl<is_const>::TimeSeriesHistogramPayloadColumnsViewImpl(Ref<ColumnTuple> tuple)
    : TimeSeriesHistogramPayloadColumnsViewImpl(FromSource{}, TupleSource{tuple})
{
}

template <bool is_const>
TimeSeriesHistogramPayloadColumnsViewImpl<is_const>::TimeSeriesHistogramPayloadColumnsViewImpl(const Columns & payload) requires (!is_const)
    : TimeSeriesHistogramPayloadColumnsViewImpl(FromSource{}, BuiltColumnsSource{nullptr, payload})
{
}

template <bool is_const>
TimeSeriesHistogramPayloadColumnsViewImpl<is_const>::TimeSeriesHistogramPayloadColumnsViewImpl(
    const TimeSeriesHistogramPayloadColumnsViewImpl<!is_const> & other) requires is_const
    : TimeSeriesHistogramPayloadColumnsViewImpl(FromSource{}, ViewSource{other})
{
}

template <bool is_const>
typename TimeSeriesHistogramPayloadColumnsViewImpl<is_const>::Columns
TimeSeriesHistogramPayloadColumnsViewImpl<is_const>::createPayloadColumns() requires (!is_const)
{
    Columns result;
    for (const auto element : Registry::getAll())
        result[static_cast<size_t>(element)] = Registry::getDataType(element)->createColumn();
    return result;
}

template <bool is_const>
typename TimeSeriesHistogramPayloadColumnsViewImpl<is_const>::template Ref<IColumn>
TimeSeriesHistogramPayloadColumnsViewImpl<is_const>::getPayloadColumn(Element element) const
{
    switch (element)
    {
        case Element::IsFloat: return is_float;
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
}

template <bool is_const>
typename TimeSeriesHistogramPayloadColumnsViewImpl<is_const>::PayloadColumns
TimeSeriesHistogramPayloadColumnsViewImpl<is_const>::getPayloadColumnsInStoredOrder() const
{
    /// The stored order of the payload is the canonical order of the registry: `getTuplePosition` follows the enum.
    PayloadColumns result;
    for (const auto element : Registry::getAll())
        result[static_cast<size_t>(element)] = &getPayloadColumn(element);
    return result;
}

template <bool is_const>
TimeSeriesHistogramBucketPairs<Int64> TimeSeriesHistogramPayloadColumnsViewImpl<is_const>::intPairsAt(const ColumnArray & pairs, size_t row)
{
    return pairsAt<Int64>(pairs, row);
}

template <bool is_const>
TimeSeriesHistogramBucketPairs<Float64> TimeSeriesHistogramPayloadColumnsViewImpl<is_const>::floatPairsAt(const ColumnArray & pairs, size_t row)
{
    return pairsAt<Float64>(pairs, row);
}

template <bool is_const>
std::span<const Float64> TimeSeriesHistogramPayloadColumnsViewImpl<is_const>::floatsAt(const ColumnArray & values, size_t row)
{
    const auto & data = assert_cast<const ColumnFloat64 &>(values.getData()).getData();
    return {data.data() + values.getOffset(row), values.getSize(row)};
}

template <bool is_const>
void TimeSeriesHistogramPayloadColumnsViewImpl<is_const>::insertPayloadFrom(
    const TimeSeriesHistogramPayloadColumnsViewImpl<true> & source, size_t row) const requires (!is_const)
{
    for (const auto element : Registry::getAll())
        getPayloadColumn(element).insertFrom(source.getPayloadColumn(element), row);
}


template <bool is_const>
template <typename Source>
TimeSeriesHistogramsColumnsViewImpl<is_const>::TimeSeriesHistogramsColumnsViewImpl(typename Base::FromSource from_source, const Source & source)
    : Base(from_source, source)
    , timestamps(source.timestamps())
    , outer_offsets(source.outerOffsets())
{
    checkTimestamps(timestamps);
}

template <bool is_const>
TimeSeriesHistogramsColumnsViewImpl<is_const>::TimeSeriesHistogramsColumnsViewImpl(Ref<ColumnTuple> tuple)
    : TimeSeriesHistogramsColumnsViewImpl(typename Base::FromSource{}, TupleSource{tuple})
{
}

template <bool is_const>
TimeSeriesHistogramsColumnsViewImpl<is_const>::TimeSeriesHistogramsColumnsViewImpl(const ColumnArray & array) requires is_const
    : TimeSeriesHistogramsColumnsViewImpl(typename Base::FromSource{}, ArraySource{array})
{
}

template <bool is_const>
TimeSeriesHistogramsColumnsViewImpl<is_const>::TimeSeriesHistogramsColumnsViewImpl(IColumn & timestamps_, const Columns & payload) requires (!is_const)
    : TimeSeriesHistogramsColumnsViewImpl(typename Base::FromSource{}, BuiltColumnsSource{&timestamps_, payload})
{
}

template <bool is_const>
TimeSeriesHistogramsColumnsViewImpl<is_const>::TimeSeriesHistogramsColumnsViewImpl(
    const TimeSeriesHistogramsColumnsViewImpl<!is_const> & other) requires is_const
    : TimeSeriesHistogramsColumnsViewImpl(typename Base::FromSource{}, ViewSource{other})
{
}

template <bool is_const>
typename TimeSeriesHistogramsColumnsViewImpl<is_const>::StoredColumns
TimeSeriesHistogramsColumnsViewImpl<is_const>::getColumnsInStoredOrder() const
{
    StoredColumns result;
    result[Registry::getTimestampPosition()] = &timestamps;
    const auto payload = Base::getPayloadColumnsInStoredOrder();
    for (const auto element : Registry::getAll())
        result[Registry::getTuplePosition(element)] = payload[static_cast<size_t>(element)];
    return result;
}


template class TimeSeriesHistogramPayloadColumnsViewImpl<true>;
template class TimeSeriesHistogramPayloadColumnsViewImpl<false>;
template class TimeSeriesHistogramsColumnsViewImpl<true>;
template class TimeSeriesHistogramsColumnsViewImpl<false>;

}
