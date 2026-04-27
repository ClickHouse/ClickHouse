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

// ─── Data structs ────────────────────────────────────────────────────────────

template <typename ValueType, typename TimestampType>
struct AggregateFunctionLastChangeAtData
{
    using StoredType = ValueType;

    ValueType prev_value{};
    TimestampType prev_value_last_ts{};
    ValueType new_value{};
    TimestampType new_value_first_ts{};
    TimestampType new_value_last_ts{};
    bool seen = false;

    static bool storedEquals(const ValueType & a, const ValueType & b) { return a == b; }
    static void copyStored(ValueType & dst, const ValueType & src) { dst = src; }
    static void copyData(AggregateFunctionLastChangeAtData & dst, const AggregateFunctionLastChangeAtData & src) { dst = src; }
    static bool argEquals(const ValueType & arg, const ValueType & stored) { return arg == stored; }
    static void copyFromArg(ValueType & stored, const ValueType & arg) { stored = arg; }
};

template <typename BitmapIntType, typename TimestampType>
struct AggregateFunctionLastChangeBitmapTimeData
{
    using BitmapData = AggregateFunctionGroupBitmapData<BitmapIntType>;
    using StoredType = std::unique_ptr<BitmapData>;

    std::unique_ptr<BitmapData> prev_value;
    TimestampType prev_value_last_ts{};
    std::unique_ptr<BitmapData> new_value;
    TimestampType new_value_first_ts{};
    TimestampType new_value_last_ts{};
    bool seen = false;

    static void copyBitmap(std::unique_ptr<BitmapData> & dst, const BitmapData & src)
    {
        dst = std::make_unique<BitmapData>();
        dst->roaring_bitmap_with_small_set.merge(src.roaring_bitmap_with_small_set);
    }

    static bool bitmapsEqual(const BitmapData & a, const BitmapData & b)
    {
        return a.roaring_bitmap_with_small_set.rb_xor_cardinality(b.roaring_bitmap_with_small_set) == 0;
    }

    static bool storedEquals(const StoredType & a, const StoredType & b) { return bitmapsEqual(*a, *b); }
    static void copyStored(StoredType & dst, const StoredType & src) { copyBitmap(dst, *src); }

    static void copyData(
        AggregateFunctionLastChangeBitmapTimeData & dst,
        const AggregateFunctionLastChangeBitmapTimeData & src)
    {
        copyBitmap(dst.prev_value, *src.prev_value);
        copyBitmap(dst.new_value, *src.new_value);
        dst.prev_value_last_ts = src.prev_value_last_ts;
        dst.new_value_first_ts = src.new_value_first_ts;
        dst.new_value_last_ts  = src.new_value_last_ts;
        dst.seen               = src.seen;
    }

    static bool argEquals(const BitmapData & arg, const StoredType & stored) { return bitmapsEqual(arg, *stored); }
    static void copyFromArg(StoredType & stored, const BitmapData & arg) { copyBitmap(stored, arg); }
};


// ─── Shared base ─────────────────────────────────────────────────────────────

template <typename DataT, typename TimestampType, typename Derived>
class AggregateFunctionLastChangeAtBase
    : public IAggregateFunctionDataHelper<DataT, Derived>
{
    using Data = DataT;

public:
    AggregateFunctionLastChangeAtBase(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<Data, Derived>{arguments, params, arguments[1]}
    {
    }

    bool allocatesMemoryInArena() const override { return false; }

    String getName() const override { return "lastChangeAt"; }

    template <typename ValueArg>
    void ALWAYS_INLINE doAdd(Data & data, const ValueArg & value, TimestampType ts) const
    {
        if (!data.seen)
        {
            Data::copyFromArg(data.prev_value, value);
            data.prev_value_last_ts = ts;
            Data::copyFromArg(data.new_value, value);
            data.new_value_first_ts = ts;
            data.new_value_last_ts  = ts;
            data.seen               = true;
        }
        else if (!Data::argEquals(value, data.new_value))
        {
            Data::copyStored(data.prev_value, data.new_value);
            data.prev_value_last_ts = data.new_value_last_ts;
            Data::copyFromArg(data.new_value, value);
            data.new_value_first_ts = ts;
            data.new_value_last_ts  = ts;
        }
        else
        {
            data.new_value_last_ts = ts;
        }
    }

    bool ALWAYS_INLINE before(const Data & lhs, const Data & rhs) const
    {
        if (lhs.new_value_first_ts < rhs.prev_value_last_ts)
            return true;
        if (lhs.new_value_first_ts == rhs.prev_value_last_ts &&
            (lhs.new_value_first_ts < rhs.new_value_first_ts || lhs.prev_value_last_ts < rhs.prev_value_last_ts))
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
            Data::copyData(a, b);
            return;
        }

        if (before(a, b))
        {
            if (!Data::storedEquals(b.prev_value, b.new_value))
            {
                Data::copyData(a, b);
                return;
            }
            else if (!Data::storedEquals(a.new_value, b.prev_value))
            {
                Data::copyStored(a.prev_value, a.new_value);
                a.prev_value_last_ts = a.new_value_last_ts;
                Data::copyStored(a.new_value, b.prev_value);
                a.new_value_first_ts = b.prev_value_last_ts;
                a.new_value_last_ts  = b.new_value_last_ts;
                return;
            }
            else if (Data::storedEquals(a.prev_value, a.new_value))
            {
                if (b.prev_value_last_ts < a.prev_value_last_ts)
                    a.prev_value_last_ts = b.prev_value_last_ts;
                if (b.new_value_first_ts > a.new_value_first_ts)
                    a.new_value_first_ts = b.new_value_first_ts;
            }

            if (b.new_value_last_ts > a.new_value_last_ts)
                a.new_value_last_ts = b.new_value_last_ts;
        }
        else if (before(b, a))
        {
            if (!Data::storedEquals(a.prev_value, a.new_value))
            {
                return;
            }
            else if (!Data::storedEquals(b.new_value, a.prev_value))
            {
                Data::copyStored(a.new_value, a.prev_value);
                a.new_value_first_ts = a.prev_value_last_ts;
                Data::copyStored(a.prev_value, b.new_value);
                a.prev_value_last_ts = b.new_value_last_ts;
                return;
            }
            else if (!Data::storedEquals(b.prev_value, b.new_value))
            {
                TimestampType buf = (a.new_value_last_ts > b.new_value_last_ts)
                    ? a.new_value_last_ts
                    : b.new_value_last_ts;
                Data::copyData(a, b);
                a.new_value_last_ts = buf;
                return;
            }
            else
            {
                if (b.prev_value_last_ts < a.prev_value_last_ts)
                    a.prev_value_last_ts = b.prev_value_last_ts;
                if (b.new_value_first_ts > a.new_value_first_ts)
                    a.new_value_first_ts = b.new_value_first_ts;
                if (b.new_value_last_ts > a.new_value_last_ts)
                    a.new_value_last_ts = b.new_value_last_ts;
            }
        }
        else
        {
            /// Overlapping ranges
            const bool b_is_later = b.new_value_first_ts >= a.new_value_first_ts;
            const Data & later    = b_is_later ? b : a;
            const Data & earlier  = b_is_later ? a : b;

            typename Data::StoredType prev_value{}, new_value{};
            TimestampType prev_value_last_ts{}, new_value_first_ts{}, new_value_last_ts{};

            if (earlier.prev_value_last_ts > later.prev_value_last_ts)
            {
                // earlier is subrange of later

                if (!Data::storedEquals(later.new_value, earlier.new_value))
                {
                    Data::copyStored(prev_value, earlier.new_value);
                    Data::copyStored(new_value, later.new_value);
                    prev_value_last_ts = earlier.new_value_last_ts;
                    new_value_first_ts = later.new_value_first_ts;
                    new_value_last_ts  = later.new_value_last_ts;
                }
                else if (!Data::storedEquals(later.new_value, earlier.prev_value))
                {
                    Data::copyStored(prev_value, earlier.prev_value);
                    Data::copyStored(new_value, later.new_value);
                    prev_value_last_ts = earlier.prev_value_last_ts;
                    new_value_first_ts = earlier.new_value_first_ts;
                    new_value_last_ts  = later.new_value_last_ts;
                    if (earlier.new_value_last_ts > later.new_value_last_ts)
                        new_value_last_ts = earlier.new_value_last_ts;
                }
                else if (!Data::storedEquals(later.prev_value, earlier.prev_value))
                {
                    Data::copyStored(prev_value, later.prev_value);
                    Data::copyStored(new_value, earlier.prev_value);
                    prev_value_last_ts = later.prev_value_last_ts;
                    new_value_first_ts = earlier.prev_value_last_ts;
                    new_value_last_ts  = later.new_value_last_ts;
                    if (earlier.new_value_last_ts > later.new_value_last_ts)
                        new_value_last_ts = earlier.new_value_last_ts;
                }
                else
                {
                    // all equals
                    Data::copyStored(prev_value, later.prev_value);
                    Data::copyStored(new_value, later.new_value);
                    prev_value_last_ts = later.new_value_last_ts;
                    new_value_first_ts = later.new_value_first_ts;
                    new_value_last_ts  = later.new_value_last_ts;
                    if (earlier.new_value_last_ts > later.new_value_last_ts)
                        new_value_last_ts = earlier.new_value_last_ts;
                }
            }
            else
            {
                // earlier and later have intersection

                if (!Data::storedEquals(later.new_value, earlier.new_value))
                {
                    Data::copyStored(prev_value, earlier.new_value);
                    Data::copyStored(new_value, later.new_value);
                    prev_value_last_ts = earlier.new_value_last_ts;
                    new_value_first_ts = later.new_value_first_ts;
                    new_value_last_ts  = later.new_value_last_ts;
                }
                else if (!Data::storedEquals(later.new_value, later.prev_value))
                {
                    Data::copyStored(prev_value, later.prev_value);
                    Data::copyStored(new_value, later.new_value);
                    prev_value_last_ts = later.prev_value_last_ts;
                    new_value_first_ts = earlier.new_value_first_ts;
                    new_value_last_ts  = later.new_value_last_ts;
                    if (earlier.new_value_last_ts > later.new_value_last_ts)
                        new_value_last_ts = earlier.new_value_last_ts;
                }
                else if (!Data::storedEquals(later.prev_value, earlier.prev_value))
                {
                    Data::copyStored(prev_value, earlier.prev_value);
                    Data::copyStored(new_value, later.prev_value);
                    prev_value_last_ts = earlier.prev_value_last_ts;
                    new_value_first_ts = later.prev_value_last_ts;
                    new_value_last_ts  = later.new_value_last_ts;
                    if (earlier.new_value_last_ts > later.new_value_last_ts)
                        new_value_last_ts = earlier.new_value_last_ts;
                }
                else
                {
                    // all equals
                    Data::copyStored(prev_value, later.prev_value);
                    Data::copyStored(new_value, later.new_value);
                    prev_value_last_ts = later.new_value_last_ts;
                    new_value_first_ts = later.new_value_first_ts;
                    new_value_last_ts  = later.new_value_last_ts;
                    if (earlier.new_value_last_ts > later.new_value_last_ts)
                        new_value_last_ts = earlier.new_value_last_ts;
                }
            }

            Data::copyStored(a.prev_value, prev_value);
            Data::copyStored(a.new_value, new_value);
            a.prev_value_last_ts = prev_value_last_ts;
            a.new_value_first_ts = new_value_first_ts;
            a.new_value_last_ts  = new_value_last_ts;
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & data = this->data(place);
        TimestampType last_change_at = Data::storedEquals(data.prev_value, data.new_value) ? data.prev_value_last_ts : data.new_value_first_ts;
        static_cast<ColumnFixedSizeHelper &>(to).template insertRawData<sizeof(TimestampType)>(
            reinterpret_cast<const char *>(&last_change_at));
    }
};


// ─── Comparable value types ──────────────────────────────────────────────────

template <typename ValueType, typename TimestampType>
class AggregateFunctionLastChangeAt final
    : public AggregateFunctionLastChangeAtBase<
        AggregateFunctionLastChangeAtData<ValueType, TimestampType>,
        TimestampType,
        AggregateFunctionLastChangeAt<ValueType, TimestampType>>
{
    using Data = AggregateFunctionLastChangeAtData<ValueType, TimestampType>;
    using Base = AggregateFunctionLastChangeAtBase<Data, TimestampType, AggregateFunctionLastChangeAt>;

public:
    using Base::Base;

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto value = unalignedLoad<ValueType>(columns[0]->getRawData().data() + row_num * sizeof(ValueType));
        auto ts    = unalignedLoad<TimestampType>(columns[1]->getRawData().data() + row_num * sizeof(TimestampType));
        this->doAdd(this->data(place), value, ts);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        const auto & data = this->data(place);
        writeBinaryLittleEndian(data.prev_value, buf);
        writeBinaryLittleEndian(data.prev_value_last_ts, buf);
        writeBinaryLittleEndian(data.new_value, buf);
        writeBinaryLittleEndian(data.new_value_first_ts, buf);
        writeBinaryLittleEndian(data.new_value_last_ts, buf);
        writeBinaryLittleEndian(data.seen, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        auto & data = this->data(place);
        readBinaryLittleEndian(data.prev_value, buf);
        readBinaryLittleEndian(data.prev_value_last_ts, buf);
        readBinaryLittleEndian(data.new_value, buf);
        readBinaryLittleEndian(data.new_value_first_ts, buf);
        readBinaryLittleEndian(data.new_value_last_ts, buf);
        readBinaryLittleEndian(data.seen, buf);
    }
};


// ─── Bitmap value type ───────────────────────────────────────────────────────

template <typename BitmapIntType, typename TimestampType>
class AggregateFunctionLastChangeBitmapTime final
    : public AggregateFunctionLastChangeAtBase<
        AggregateFunctionLastChangeBitmapTimeData<BitmapIntType, TimestampType>,
        TimestampType,
        AggregateFunctionLastChangeBitmapTime<BitmapIntType, TimestampType>>
{
    using Data   = AggregateFunctionLastChangeBitmapTimeData<BitmapIntType, TimestampType>;
    using Bitmap = AggregateFunctionGroupBitmapData<BitmapIntType>;
    using Base   = AggregateFunctionLastChangeAtBase<Data, TimestampType, AggregateFunctionLastChangeBitmapTime>;

public:
    using Base::Base;

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & col     = assert_cast<const ColumnAggregateFunction &>(*columns[0]);
        const auto & new_bmp = *reinterpret_cast<const Bitmap *>(col.getData()[row_num]);
        auto ts = unalignedLoad<TimestampType>(columns[1]->getRawData().data() + row_num * sizeof(TimestampType));
        this->doAdd(this->data(place), new_bmp, ts);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        const auto & data = this->data(place);
        writeBinaryLittleEndian(data.prev_value_last_ts, buf);
        writeBinaryLittleEndian(data.new_value_first_ts, buf);
        writeBinaryLittleEndian(data.new_value_last_ts, buf);
        writeBinaryLittleEndian(data.seen, buf);
        if (data.seen)
        {
            data.prev_value->roaring_bitmap_with_small_set.write(buf);
            data.new_value->roaring_bitmap_with_small_set.write(buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        auto & data = this->data(place);
        readBinaryLittleEndian(data.prev_value_last_ts, buf);
        readBinaryLittleEndian(data.new_value_first_ts, buf);
        readBinaryLittleEndian(data.new_value_last_ts, buf);
        readBinaryLittleEndian(data.seen, buf);
        if (data.seen)
        {
            data.prev_value = std::make_unique<Bitmap>();
            data.prev_value->roaring_bitmap_with_small_set.read(buf);
            data.new_value = std::make_unique<Bitmap>();
            data.new_value->roaring_bitmap_with_small_set.read(buf);
        }
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
    if (w.idx == TypeIndex::UInt8)       res = createBitmapWithTimestampType<UInt8>(ts_type, args...);
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

}
