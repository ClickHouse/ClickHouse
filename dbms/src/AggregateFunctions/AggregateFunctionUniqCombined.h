#pragma once

#include <Common/CombinedCardinalityEstimator.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/UniqCombinedBiasData.h>
#include <AggregateFunctions/UniqVariadicHash.h>

#include <ext/bit_cast.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitors.h>
#include <Common/SipHash.h>

#include <Common/typeid_cast.h>

namespace DB
{
namespace detail
{
    /** Hash function for uniqCombined.
      */
    template <typename T>
    struct AggregateFunctionUniqCombinedTraits
    {
        static UInt32 hash(T x)
        {
            return static_cast<UInt32>(intHash64(x));
        }
    };

    template <>
    struct AggregateFunctionUniqCombinedTraits<UInt128>
    {
        static UInt32 hash(UInt128 x)
        {
            return sipHash64(x);
        }
    };

    template <>
    struct AggregateFunctionUniqCombinedTraits<Float32>
    {
        static UInt32 hash(Float32 x)
        {
            UInt64 res = ext::bit_cast<UInt64>(x);
            return static_cast<UInt32>(intHash64(res));
        }
    };

    template <>
    struct AggregateFunctionUniqCombinedTraits<Float64>
    {
        static UInt32 hash(Float64 x)
        {
            UInt64 res = ext::bit_cast<UInt64>(x);
            return static_cast<UInt32>(intHash64(res));
        }
    };

} // namespace detail


template <typename Key>
struct AggregateFunctionUniqCombinedDataWithKey
{
    template <UInt8 K>
    using Set = CombinedCardinalityEstimator<Key,
        HashSet<Key, TrivialHash, HashTableGrower<>>,
        16,
        K - 3,
        K,
        TrivialHash,
        Key,
        HyperLogLogBiasEstimator<UniqCombinedBiasData>,
        HyperLogLogMode::FullFeatured>;

    mutable UInt8 inited = 0;
    union
    {
        mutable Set<12> set_12;
        mutable Set<13> set_13;
        mutable Set<14> set_14;
        mutable Set<15> set_15;
        mutable Set<16> set_16;
        mutable Set<17> set_17;
        mutable Set<18> set_18;
        mutable Set<19> set_19;
        mutable Set<20> set_20;
    };

    AggregateFunctionUniqCombinedDataWithKey() : set_17() {}

    ~AggregateFunctionUniqCombinedDataWithKey()
    {
        switch (inited)
        {
            case 12:
                set_12.~CombinedCardinalityEstimator();
                break;
            case 13:
                set_13.~CombinedCardinalityEstimator();
                break;
            case 14:
                set_14.~CombinedCardinalityEstimator();
                break;
            case 15:
                set_15.~CombinedCardinalityEstimator();
                break;
            case 16:
                set_16.~CombinedCardinalityEstimator();
                break;
            case 0:
            case 17:
                set_17.~CombinedCardinalityEstimator();
                break;
            case 18:
                set_18.~CombinedCardinalityEstimator();
                break;
            case 19:
                set_19.~CombinedCardinalityEstimator();
                break;
            case 20:
                set_20.~CombinedCardinalityEstimator();
                break;
        }
    }

    void init(UInt8 precision) const
    {
        if (inited)
            return;

        if (precision == 17)
        {
            inited = precision;
            return;
        }

        // TODO: assert "inited == precision"

        set_17.~CombinedCardinalityEstimator();

        switch (precision)
        {
            case 12:
                new (&set_12) Set<12>;
                break;
            case 13:
                new (&set_13) Set<13>;
                break;
            case 14:
                new (&set_14) Set<14>;
                break;
            case 15:
                new (&set_15) Set<15>;
                break;
            case 16:
                new (&set_16) Set<16>;
                break;
            case 18:
                new (&set_18) Set<18>;
                break;
            case 19:
                new (&set_19) Set<19>;
                break;
            case 20:
                new (&set_20) Set<20>;
                break;
        }
        inited = precision;
    }

#define SET_METHOD(method) \
    switch (inited)        \
    {                      \
        case 12:           \
            set_12.method; \
            break;         \
        case 13:           \
            set_13.method; \
            break;         \
        case 14:           \
            set_14.method; \
            break;         \
        case 15:           \
            set_15.method; \
            break;         \
        case 16:           \
            set_16.method; \
            break;         \
        case 17:           \
            set_17.method; \
            break;         \
        case 18:           \
            set_18.method; \
            break;         \
        case 19:           \
            set_19.method; \
            break;         \
        case 20:           \
            set_20.method; \
            break;         \
    }

#define SET_RETURN_METHOD(method) \
    switch (inited)               \
    {                             \
        case 12:                  \
            return set_12.method; \
        case 13:                  \
            return set_13.method; \
        case 14:                  \
            return set_14.method; \
        case 15:                  \
            return set_15.method; \
        case 16:                  \
            return set_16.method; \
        case 18:                  \
            return set_18.method; \
        case 19:                  \
            return set_19.method; \
        case 20:                  \
            return set_20.method; \
        case 17:                  \
        default:                  \
            return set_17.method; \
    }

    void insert(Key value, UInt8 precision)
    {
        init(precision);
        SET_METHOD(insert(value));
    }

    void merge(const AggregateFunctionUniqCombinedDataWithKey<Key> & rhs, UInt8 precision)
    {
        init(precision);
        switch (inited)
        {
            case 12:
                set_12.merge(rhs.set_12);
                break;
            case 13:
                set_13.merge(rhs.set_13);
                break;
            case 14:
                set_14.merge(rhs.set_14);
                break;
            case 15:
                set_15.merge(rhs.set_15);
                break;
            case 16:
                set_16.merge(rhs.set_16);
                break;
            case 17:
                set_17.merge(rhs.set_17);
                break;
            case 18:
                set_18.merge(rhs.set_18);
                break;
            case 19:
                set_19.merge(rhs.set_19);
                break;
            case 20:
                set_20.merge(rhs.set_20);
                break;
        }
    }

    void write(DB::WriteBuffer & out, UInt8 precision) const
    {
        init(precision);
        SET_METHOD(write(out));
    }

    void read(DB::ReadBuffer & in, UInt8 precision)
    {
        init(precision);
        SET_METHOD(read(in));
    }

    UInt32 size(UInt8 precision) const
    {
        init(precision);
        SET_RETURN_METHOD(size());
    }

#undef SET_METHOD
#undef SET_RETURN_METHOD
};


template <typename T>
struct __attribute__((__packed__)) AggregateFunctionUniqCombinedData : public AggregateFunctionUniqCombinedDataWithKey<UInt32>
{
};


template <>
struct __attribute__((__packed__)) AggregateFunctionUniqCombinedData<String> : public AggregateFunctionUniqCombinedDataWithKey<UInt64>
{
};


template <typename T>
class AggregateFunctionUniqCombined final
    : public IAggregateFunctionDataHelper<AggregateFunctionUniqCombinedData<T>, AggregateFunctionUniqCombined<T>>
{
private:
    const UInt8 precision;

public:
    explicit AggregateFunctionUniqCombined(UInt8 precision) : precision(precision) {}

    String getName() const override
    {
        return "uniqCombined";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (!std::is_same_v<T, String>)
        {
            const auto & value = static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
            this->data(place).insert(detail::AggregateFunctionUniqCombinedTraits<T>::hash(value), precision);
        }
        else
        {
            StringRef value = columns[0]->getDataAt(row_num);
            this->data(place).insert(CityHash_v1_0_2::CityHash64(value.data, value.size), precision);
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs), precision);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf, precision);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).read(buf, precision);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).size(precision));
    }

    const char * getHeaderFilePath() const override
    {
        return __FILE__;
    }
};

/** For multiple arguments. To compute, hashes them.
  * You can pass multiple arguments as is; You can also pass one argument - a tuple.
  * But (for the possibility of efficient implementation), you can not pass several arguments, among which there are tuples.
  */
template <bool is_exact, bool argument_is_tuple>
class AggregateFunctionUniqCombinedVariadic final : public IAggregateFunctionDataHelper<AggregateFunctionUniqCombinedData<UInt64>,
                                                        AggregateFunctionUniqCombinedVariadic<is_exact, argument_is_tuple>>
{
private:
    size_t num_args = 0;
    UInt8 precision;

public:
    AggregateFunctionUniqCombinedVariadic(const DataTypes & arguments, UInt8 precision) : precision(precision)
    {
        if (argument_is_tuple)
            num_args = typeid_cast<const DataTypeTuple &>(*arguments[0]).getElements().size();
        else
            num_args = arguments.size();
    }

    String getName() const override
    {
        return "uniqCombined";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).insert(typename AggregateFunctionUniqCombinedData<UInt64>::Set<12>::value_type(
                                     UniqVariadicHash<is_exact, argument_is_tuple>::apply(num_args, columns, row_num)),
            precision);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs), precision);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf, precision);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).read(buf, precision);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).size(precision));
    }

    const char * getHeaderFilePath() const override
    {
        return __FILE__;
    }
};

} // namespace DB
