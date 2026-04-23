#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeAggregateFunction.h>

#include <Columns/ColumnAggregateFunction.h>

#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <memory>

// Include last: the roaring header pollutes the preprocessor environment.
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

// ─── Comparable value types ─────────────────────────────────────────────────

/// Tracks the timestamp of the last actual change in `value` across an
/// ordered sequence.  Identical consecutive values are not counted as a change.
/// When no change is ever observed, returns the first timestamp seen.
template <typename ValueType, typename TimestampType>
struct AggregateFunctionLastChangeAtData
{
    ValueType first_value{};
    ValueType last_value{};
    TimestampType first_ts{};
    TimestampType last_ts{};
    TimestampType last_change_ts{};
    bool has_changes = false;
    bool seen = false;
};

template <typename ValueType, typename TimestampType>
class AggregateFunctionLastChangeAt final
    : public IAggregateFunctionDataHelper<
        AggregateFunctionLastChangeAtData<ValueType, TimestampType>,
        AggregateFunctionLastChangeAt<ValueType, TimestampType>>
{
    using Data = AggregateFunctionLastChangeAtData<ValueType, TimestampType>;

public:
    AggregateFunctionLastChangeAt(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionLastChangeAt>{
            arguments, params, arguments[1]}
    {
    }

    bool allocatesMemoryInArena() const override { return false; }

    String getName() const override { return "lastChangeAt"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto value = unalignedLoad<ValueType>(columns[0]->getRawData().data() + row_num * sizeof(ValueType));
        auto ts    = unalignedLoad<TimestampType>(columns[1]->getRawData().data() + row_num * sizeof(TimestampType));

        auto & data = this->data(place);

        if (!data.seen)
        {
            data.first_value = value;
            data.first_ts    = ts;
            data.last_value  = value;
            data.last_ts     = ts;
            data.seen        = true;
        }
        else
        {
            if (value != data.last_value)
            {
                data.last_change_ts = ts;
                data.has_changes    = true;
            }
            data.last_value = value;
            data.last_ts    = ts;
        }
    }

    /// Returns true if lhs time range is entirely before rhs time range.
    bool ALWAYS_INLINE before(const Data & lhs, const Data & rhs) const
    {
        if (lhs.last_ts < rhs.first_ts)
            return true;
        if (lhs.last_ts == rhs.first_ts && (lhs.last_ts < rhs.last_ts || lhs.first_ts < rhs.first_ts))
            return true;
        return false;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs_ptr, Arena *) const override
    {
        auto & a       = this->data(place);
        const auto & b = this->data(rhs_ptr);

        if (!b.seen)
            return;

        if (!a.seen)
        {
            a = b;
            return;
        }

        if (before(a, b))
        {
            /// a's range is entirely before b's.
            const bool boundary_changed = (b.first_value != a.last_value);

            if (b.has_changes)
            {
                a.last_change_ts = b.last_change_ts;
                a.has_changes    = true;
            }
            else if (boundary_changed)
            {
                a.last_change_ts = b.first_ts;
                a.has_changes    = true;
            }

            a.last_value = b.last_value;
            a.last_ts    = b.last_ts;
        }
        else if (before(b, a))
        {
            /// b's range is entirely before a's.
            const bool boundary_changed = (a.first_value != b.last_value);

            TimestampType new_last_change_ts = a.last_change_ts;
            bool new_has_changes             = a.has_changes;

            if (!a.has_changes && boundary_changed)
            {
                new_last_change_ts = a.first_ts;
                new_has_changes    = true;
            }
            else if (!a.has_changes)
            {
                new_last_change_ts = b.last_change_ts;
                new_has_changes    = b.has_changes;
            }

            a.first_value    = b.first_value;
            a.first_ts       = b.first_ts;
            a.last_change_ts = new_last_change_ts;
            a.has_changes    = new_has_changes;
        }
        else
        {
            /// Overlapping ranges — keep the state with the later last_ts for determinism.
            if (b.last_ts > a.last_ts)
                a = b;
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & data = this->data(place);
        writeBinaryLittleEndian(data.first_value, buf);
        writeBinaryLittleEndian(data.last_value, buf);
        writeBinaryLittleEndian(data.first_ts, buf);
        writeBinaryLittleEndian(data.last_ts, buf);
        writeBinaryLittleEndian(data.last_change_ts, buf);
        writeBinaryLittleEndian(data.has_changes, buf);
        writeBinaryLittleEndian(data.seen, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & data = this->data(place);
        readBinaryLittleEndian(data.first_value, buf);
        readBinaryLittleEndian(data.last_value, buf);
        readBinaryLittleEndian(data.first_ts, buf);
        readBinaryLittleEndian(data.last_ts, buf);
        readBinaryLittleEndian(data.last_change_ts, buf);
        readBinaryLittleEndian(data.has_changes, buf);
        readBinaryLittleEndian(data.seen, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & data = this->data(place);
        TimestampType result = data.has_changes ? data.last_change_ts : data.first_ts;
        static_cast<ColumnFixedSizeHelper &>(to).template insertRawData<sizeof(TimestampType)>(
            reinterpret_cast<const char *>(&result));
    }
};


// ─── Bitmap value type ──────────────────────────────────────────────────────

/// State for the bitmap specialisation.  Bitmaps are stored as heap-allocated
/// objects and treated as equal when bitmapXorCardinality == 0.
template <typename BitmapIntType, typename TimestampType>
struct AggregateFunctionLastChangeBitmapTimeData
{
    using BitmapData = AggregateFunctionGroupBitmapData<BitmapIntType>;

    std::unique_ptr<BitmapData> first_value;
    std::unique_ptr<BitmapData> last_value;
    TimestampType first_ts{};
    TimestampType last_ts{};
    TimestampType last_change_ts{};
    bool has_changes = false;
    bool seen        = false;

    static void copyBitmap(std::unique_ptr<BitmapData> & dst, const BitmapData & src)
    {
        dst = std::make_unique<BitmapData>();
        dst->roaring_bitmap_with_small_set.merge(src.roaring_bitmap_with_small_set);
    }

    static bool bitmapsEqual(const BitmapData & a, const BitmapData & b)
    {
        return a.roaring_bitmap_with_small_set.rb_xor_cardinality(
                   b.roaring_bitmap_with_small_set) == 0;
    }
};

template <typename BitmapIntType, typename TimestampType>
class AggregateFunctionLastChangeBitmapTime final
    : public IAggregateFunctionDataHelper<
        AggregateFunctionLastChangeBitmapTimeData<BitmapIntType, TimestampType>,
        AggregateFunctionLastChangeBitmapTime<BitmapIntType, TimestampType>>
{
    using Data    = AggregateFunctionLastChangeBitmapTimeData<BitmapIntType, TimestampType>;
    using Bitmap  = AggregateFunctionGroupBitmapData<BitmapIntType>;

public:
    AggregateFunctionLastChangeBitmapTime(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionLastChangeBitmapTime>{
            arguments, params, arguments[1]}
    {
    }

    bool allocatesMemoryInArena() const override { return false; }

    String getName() const override { return "lastChangeAt"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & col       = assert_cast<const ColumnAggregateFunction &>(*columns[0]);
        const auto & new_bmp   = *reinterpret_cast<const Bitmap *>(col.getData()[row_num]);
        auto ts = unalignedLoad<TimestampType>(columns[1]->getRawData().data() + row_num * sizeof(TimestampType));

        auto & data = this->data(place);

        if (!data.seen)
        {
            Data::copyBitmap(data.first_value, new_bmp);
            Data::copyBitmap(data.last_value, new_bmp);
            data.first_ts = ts;
            data.last_ts  = ts;
            data.seen     = true;
        }
        else
        {
            if (!Data::bitmapsEqual(*data.last_value, new_bmp))
            {
                data.last_change_ts = ts;
                data.has_changes    = true;
            }
            Data::copyBitmap(data.last_value, new_bmp);
            data.last_ts = ts;
        }
    }

    bool ALWAYS_INLINE before(const Data & lhs, const Data & rhs) const
    {
        if (lhs.last_ts < rhs.first_ts)
            return true;
        if (lhs.last_ts == rhs.first_ts && (lhs.last_ts < rhs.last_ts || lhs.first_ts < rhs.first_ts))
            return true;
        return false;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs_ptr, Arena *) const override
    {
        auto & a       = this->data(place);
        const auto & b = this->data(rhs_ptr);

        if (!b.seen)
            return;

        if (!a.seen)
        {
            Data::copyBitmap(a.first_value, *b.first_value);
            Data::copyBitmap(a.last_value, *b.last_value);
            a.first_ts       = b.first_ts;
            a.last_ts        = b.last_ts;
            a.last_change_ts = b.last_change_ts;
            a.has_changes    = b.has_changes;
            a.seen           = true;
            return;
        }

        if (before(a, b))
        {
            const bool boundary_changed = !Data::bitmapsEqual(*b.first_value, *a.last_value);

            if (b.has_changes)
            {
                a.last_change_ts = b.last_change_ts;
                a.has_changes    = true;
            }
            else if (boundary_changed)
            {
                a.last_change_ts = b.first_ts;
                a.has_changes    = true;
            }

            Data::copyBitmap(a.last_value, *b.last_value);
            a.last_ts = b.last_ts;
        }
        else if (before(b, a))
        {
            const bool boundary_changed = !Data::bitmapsEqual(*a.first_value, *b.last_value);

            TimestampType new_last_change_ts = a.last_change_ts;
            bool new_has_changes             = a.has_changes;

            if (!a.has_changes && boundary_changed)
            {
                new_last_change_ts = a.first_ts;
                new_has_changes    = true;
            }
            else if (!a.has_changes)
            {
                new_last_change_ts = b.last_change_ts;
                new_has_changes    = b.has_changes;
            }

            Data::copyBitmap(a.first_value, *b.first_value);
            a.first_ts       = b.first_ts;
            a.last_change_ts = new_last_change_ts;
            a.has_changes    = new_has_changes;
        }
        else
        {
            if (b.last_ts > a.last_ts)
            {
                Data::copyBitmap(a.first_value, *b.first_value);
                Data::copyBitmap(a.last_value, *b.last_value);
                a.first_ts       = b.first_ts;
                a.last_ts        = b.last_ts;
                a.last_change_ts = b.last_change_ts;
                a.has_changes    = b.has_changes;
            }
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & data = this->data(place);
        writeBinaryLittleEndian(data.first_ts, buf);
        writeBinaryLittleEndian(data.last_ts, buf);
        writeBinaryLittleEndian(data.last_change_ts, buf);
        writeBinaryLittleEndian(data.has_changes, buf);
        writeBinaryLittleEndian(data.seen, buf);
        if (data.seen)
        {
            data.first_value->roaring_bitmap_with_small_set.write(buf);
            data.last_value->roaring_bitmap_with_small_set.write(buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & data = this->data(place);
        readBinaryLittleEndian(data.first_ts, buf);
        readBinaryLittleEndian(data.last_ts, buf);
        readBinaryLittleEndian(data.last_change_ts, buf);
        readBinaryLittleEndian(data.has_changes, buf);
        readBinaryLittleEndian(data.seen, buf);
        if (data.seen)
        {
            data.first_value = std::make_unique<Bitmap>();
            data.first_value->roaring_bitmap_with_small_set.read(buf);
            data.last_value = std::make_unique<Bitmap>();
            data.last_value->roaring_bitmap_with_small_set.read(buf);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & data = this->data(place);
        TimestampType result = data.has_changes ? data.last_change_ts : data.first_ts;
        static_cast<ColumnFixedSizeHelper &>(to).template insertRawData<sizeof(TimestampType)>(
            reinterpret_cast<const char *>(&result));
    }
};


// ─── Type dispatch ───────────────────────────────────────────────────────────

template <typename ValueType, template <typename, typename> class Tmpl, typename... TArgs>
IAggregateFunction * createWithTimestampType(const IDataType & ts_type, TArgs && ... args)
{
    WhichDataType w(ts_type);
    if (w.idx == TypeIndex::UInt8)      return new Tmpl<ValueType, UInt8>(args...);
    if (w.idx == TypeIndex::UInt16)     return new Tmpl<ValueType, UInt16>(args...);
    if (w.idx == TypeIndex::UInt32)     return new Tmpl<ValueType, UInt32>(args...);
    if (w.idx == TypeIndex::UInt64)     return new Tmpl<ValueType, UInt64>(args...);
    if (w.idx == TypeIndex::Int8)       return new Tmpl<ValueType, Int8>(args...);
    if (w.idx == TypeIndex::Int16)      return new Tmpl<ValueType, Int16>(args...);
    if (w.idx == TypeIndex::Int32)      return new Tmpl<ValueType, Int32>(args...);
    if (w.idx == TypeIndex::Int64)      return new Tmpl<ValueType, Int64>(args...);
    if (w.idx == TypeIndex::Float32)    return new Tmpl<ValueType, Float32>(args...);
    if (w.idx == TypeIndex::Float64)    return new Tmpl<ValueType, Float64>(args...);
    if (w.idx == TypeIndex::Date)       return new Tmpl<ValueType, UInt16>(args...);
    if (w.idx == TypeIndex::DateTime)   return new Tmpl<ValueType, UInt32>(args...);
    if (w.idx == TypeIndex::DateTime64) return new Tmpl<ValueType, DateTime64>(args...);
    return nullptr;
}

template <template <typename, typename> class Tmpl, typename... TArgs>
IAggregateFunction * createWithValueAndTimestampTypes(
    const IDataType & val_type, const IDataType & ts_type, TArgs && ... args)
{
    WhichDataType w(val_type);
    if (w.idx == TypeIndex::UInt8)     return createWithTimestampType<UInt8, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::UInt16)    return createWithTimestampType<UInt16, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::UInt32)    return createWithTimestampType<UInt32, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::UInt64)    return createWithTimestampType<UInt64, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::Int8)      return createWithTimestampType<Int8, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::Int16)     return createWithTimestampType<Int16, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::Int32)     return createWithTimestampType<Int32, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::Int64)     return createWithTimestampType<Int64, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::Float32)   return createWithTimestampType<Float32, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::Float64)   return createWithTimestampType<Float64, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::Decimal32) return createWithTimestampType<Decimal32, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::Decimal64) return createWithTimestampType<Decimal64, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::Decimal128) return createWithTimestampType<Decimal128, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::Decimal256) return createWithTimestampType<Decimal256, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::DateTime64) return createWithTimestampType<DateTime64, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::Date)      return createWithTimestampType<UInt16, Tmpl>(ts_type, args...);
    if (w.idx == TypeIndex::DateTime)  return createWithTimestampType<UInt32, Tmpl>(ts_type, args...);
    return nullptr;
}

template <typename BitmapIntType, typename... TArgs>
IAggregateFunction * createBitmapWithTimestampType(const IDataType & ts_type, TArgs && ... args)
{
    WhichDataType w(ts_type);
    if (w.idx == TypeIndex::UInt8)      return new AggregateFunctionLastChangeBitmapTime<BitmapIntType, UInt8>(args...);
    if (w.idx == TypeIndex::UInt16)     return new AggregateFunctionLastChangeBitmapTime<BitmapIntType, UInt16>(args...);
    if (w.idx == TypeIndex::UInt32)     return new AggregateFunctionLastChangeBitmapTime<BitmapIntType, UInt32>(args...);
    if (w.idx == TypeIndex::UInt64)     return new AggregateFunctionLastChangeBitmapTime<BitmapIntType, UInt64>(args...);
    if (w.idx == TypeIndex::Int8)       return new AggregateFunctionLastChangeBitmapTime<BitmapIntType, Int8>(args...);
    if (w.idx == TypeIndex::Int16)      return new AggregateFunctionLastChangeBitmapTime<BitmapIntType, Int16>(args...);
    if (w.idx == TypeIndex::Int32)      return new AggregateFunctionLastChangeBitmapTime<BitmapIntType, Int32>(args...);
    if (w.idx == TypeIndex::Int64)      return new AggregateFunctionLastChangeBitmapTime<BitmapIntType, Int64>(args...);
    if (w.idx == TypeIndex::Float32)    return new AggregateFunctionLastChangeBitmapTime<BitmapIntType, Float32>(args...);
    if (w.idx == TypeIndex::Float64)    return new AggregateFunctionLastChangeBitmapTime<BitmapIntType, Float64>(args...);
    if (w.idx == TypeIndex::Date)       return new AggregateFunctionLastChangeBitmapTime<BitmapIntType, UInt16>(args...);
    if (w.idx == TypeIndex::DateTime)   return new AggregateFunctionLastChangeBitmapTime<BitmapIntType, UInt32>(args...);
    if (w.idx == TypeIndex::DateTime64) return new AggregateFunctionLastChangeBitmapTime<BitmapIntType, DateTime64>(args...);
    return nullptr;
}

template <typename... TArgs>
IAggregateFunction * createBitmapWithIntType(
    const String & name, const IDataType & bitmap_int_type, const IDataType & ts_type, TArgs && ... args)
{
    WhichDataType w(bitmap_int_type);
    IAggregateFunction * res = nullptr;
    if (w.idx == TypeIndex::UInt8)  res = createBitmapWithTimestampType<UInt8>(ts_type, args...);
    else if (w.idx == TypeIndex::UInt16) res = createBitmapWithTimestampType<UInt16>(ts_type, args...);
    else if (w.idx == TypeIndex::UInt32) res = createBitmapWithTimestampType<UInt32>(ts_type, args...);
    else if (w.idx == TypeIndex::UInt64) res = createBitmapWithTimestampType<UInt64>(ts_type, args...);
    else
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Unsupported groupBitmap integer type {} for aggregate function {}; "
            "use UInt8, UInt16, UInt32, or UInt64",
            bitmap_int_type.getName(), name);
    if (!res)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Unsupported timestamp type {} for aggregate function {}; "
            "use (U)Int*, Float*, Date, DateTime, or DateTime64",
            ts_type.getName(), name);
    return res;
}


// ─── Factory ─────────────────────────────────────────────────────────────────

AggregateFunctionPtr createAggregateFunctionLastChangeAt(
    const String & name,
    const DataTypes & arguments,
    const Array & params,
    const Settings *)
{
    assertNoParameters(name, params);
    assertBinary(name, arguments);

    const IDataType & val_type = *arguments[0];
    const IDataType & ts_type  = *arguments[1];

    WhichDataType which_ts(ts_type);
    if (!which_ts.isInteger() && !which_ts.isFloat() && !which_ts.isDate()
        && !which_ts.isDateTime() && !which_ts.isDateTime64())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of second argument for aggregate function {}; "
            "must be Int, Float, Date, DateTime, or DateTime64",
            ts_type.getName(), name);

    // Bitmap value path
    if (const auto * agg_type = typeid_cast<const DataTypeAggregateFunction *>(&val_type))
    {
        if (agg_type->getFunctionName() != "groupBitmap")
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal AggregateFunction type {} of first argument for aggregate function {}; "
                "only groupBitmap is supported",
                val_type.getName(), name);

        const DataTypes & bitmap_arg_types = agg_type->getArgumentsDataTypes();
        if (bitmap_arg_types.empty())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "groupBitmap AggregateFunction has no argument types in {}",
                name);

        return AggregateFunctionPtr(createBitmapWithIntType(name, *bitmap_arg_types[0], ts_type, arguments, params));
    }

    // Comparable value path
    WhichDataType which_val(val_type);
    if (!which_val.isInteger() && !which_val.isFloat() && !which_val.isDecimal()
        && !which_val.isDateTime64() && !which_val.isDate() && !which_val.isDateTime())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of first argument for aggregate function {}; "
            "must be Int, Float, Decimal, Date, DateTime, DateTime64, "
            "or AggregateFunction(groupBitmap, UInt*)",
            val_type.getName(), name);

    auto * res = createWithValueAndTimestampTypes<AggregateFunctionLastChangeAt>(
        val_type, ts_type, arguments, params);
    if (!res)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Unsupported type combination ({}, {}) for aggregate function {}",
            val_type.getName(), ts_type.getName(), name);

    return AggregateFunctionPtr(res);
}

} // anonymous namespace

void registerAggregateFunctionLastChangeAt(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Returns the `calculated_at` timestamp of the most recent actual change in `value` across a
time-ordered sequence.  A change is counted only when the value genuinely differs between
consecutive observations (e.g. `0` → `1` counts; `0` → `0` does not).  If the value never
changes, the first `calculated_at` is returned.

Designed for [`AggregatingMergeTree`](/engines/table-engines/mergetree-family/aggregatingmergetree)
to maintain a `changed_at` column without storing full history.

Supported value types: `(U)Int*`, `Float*`, `Decimal*`, `Date`, `DateTime`, `DateTime64`,
and `AggregateFunction(groupBitmap, UInt*)` — for bitmaps, equality is tested via
`bitmapXorCardinality`.
    )";
    FunctionDocumentation::Syntax syntax = "lastChangeAt(value, calculated_at)";
    FunctionDocumentation::Arguments arguments = {
        {"value",
         "Metric value to track. Comparable types use `!=`; bitmaps use `bitmapXorCardinality`.",
         {"(U)Int*", "Float*", "Decimal*", "Date", "DateTime", "DateTime64",
          "AggregateFunction(groupBitmap, UInt*)"}},
        {"calculated_at",
         "Timestamp of each observation. Input rows must be sorted by this column.",
         {"(U)Int*", "Float*", "Date", "DateTime", "DateTime64"}},
    };
    FunctionDocumentation::Parameters parameters = {};
    FunctionDocumentation::ReturnedValue returned_value = {
        "The `calculated_at` of the last actual change. "
        "Returns the first `calculated_at` if the value never changed.",
        {"(U)Int*", "Float*", "Date", "DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage",
         R"(
-- Value changes at ts=3 (0→1) then stays constant. Result is 3, not 5.
SELECT lastChangeAt(value, ts)
FROM (SELECT number AS ts, [0, 0, 1, 1, 1][number] AS value FROM numbers(1, 5))
         )",
         R"(
┌─lastChangeAt(value, ts)─┐
│                         3 │
└───────────────────────────┘
         )"}};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {
        description, syntax, arguments, parameters, returned_value, examples, {}, category};
    AggregateFunctionProperties properties = {
        .returns_default_when_only_null = true, .is_order_dependent = true};

    factory.registerFunction("lastChangeAt",
        {createAggregateFunctionLastChangeAt, documentation, properties});
}

} // namespace DB
