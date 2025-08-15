#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Core/Field.h>
#include <Common/HashTable/HashMap.h>
#include <Common/NaNUtils.h>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/** Approximates Quantile by:
  * - sorting input values and weights
  * - building a cumulative distribution based on weights
  * - performing linear interpolation between the weights and values
  *
  */
template <typename Value>
struct QuantileInterpolatedWeighted
{
    struct Int128Hash
    {
        size_t operator()(Int128 x) const
        {
            return CityHash_v1_0_2::Hash128to64({x >> 64, x & 0xffffffffffffffffll});
        }
    };

    using Weight = UInt64;
    using UnderlyingType = NativeType<Value>;
    using Hasher = HashCRC32<UnderlyingType>;

    /// When creating, the hash table must be small.
    using Map = HashMapWithStackMemory<UnderlyingType, Weight, Hasher, 4>;

    Map map;

    void add(const Value & x)
    {
        /// We must skip NaNs as they are not compatible with comparison sorting.
        if (!isNaN(x))
            ++map[x];
    }

    void add(const Value & x, Weight weight)
    {
        if (!isNaN(x))
            map[x] += weight;
    }

    void merge(const QuantileInterpolatedWeighted & rhs)
    {
        for (const auto & pair : rhs.map)
            map[pair.getKey()] += pair.getMapped();
    }

    void serialize(WriteBuffer & buf) const
    {
        map.write(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        typename Map::Reader reader(buf);
        while (reader.next())
        {
            const auto & pair = reader.get();
            map[pair.first] = pair.second;
        }
    }

    Value get(Float64 level) const
    {
        return getImpl<Value>(level);
    }

    void getMany(const Float64 * levels, const size_t * indices, size_t size, Value * result) const
    {
        getManyImpl<Value>(levels, indices, size, result);
    }

    /// The same, but in the case of an empty state, NaN is returned.
    Float64 getFloat(Float64) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getFloat is not implemented for QuantileInterpolatedWeighted");
    }

    void getManyFloat(const Float64 *, const size_t *, size_t, Float64 *) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getManyFloat is not implemented for QuantileInterpolatedWeighted");
    }

private:
    using Pair = typename std::pair<UnderlyingType, Float64>;

    /// Get the value of the `level` quantile. The level must be between 0 and 1.
    template <typename T>
    T getImpl(Float64 level) const
    {
        size_t size = map.size();

        if (0 == size)
            return std::numeric_limits<Value>::quiet_NaN();

        /// Maintain a vector of pair of values and weights for easier sorting and for building
        /// a cumulative distribution using the provided weights.
        std::vector<Pair> value_weight_pairs;
        value_weight_pairs.reserve(size);

        /// Note: weight provided must be a 64-bit integer
        /// Float64 is used as accumulator here to get approximate results.
        /// But weight used in the internal array is stored as Float64 as we
        /// do some quantile estimation operation which involves division and
        /// require Float64 level of precision.

        Float64 sum_weight = 0;
        for (const auto & pair : map)
        {
            sum_weight += pair.getMapped();
            auto value = pair.getKey();
            auto weight = pair.getMapped();
            value_weight_pairs.push_back({value, weight});
        }

        ::sort(value_weight_pairs.begin(), value_weight_pairs.end(), [](const Pair & a, const Pair & b) { return a.first < b.first; });

        Float64 accumulated = 0;

        /// vector for populating and storing the cumulative sum using the provided weights.
        /// example: [0,1,2,3,4,5] -> [0,1,3,6,10,15]
        std::vector<Float64> weights_cum_sum;
        weights_cum_sum.reserve(size);

        for (size_t idx = 0; idx < size; ++idx)
        {
            accumulated += value_weight_pairs[idx].second;
            weights_cum_sum.push_back(accumulated);
        }

        /// The following estimation of quantile is general and the idea is:
        /// https://en.wikipedia.org/wiki/Percentile#The_weighted_percentile_method

        /// calculates a simple cumulative distribution based on weights
        if (sum_weight != 0)
        {
            for (size_t idx = 0; idx < size; ++idx)
                value_weight_pairs[idx].second = (weights_cum_sum[idx] - 0.5 * value_weight_pairs[idx].second) / sum_weight;
        }

        /// perform linear interpolation
        size_t idx = 0;
        if (size >= 2)
        {
            if (level >= value_weight_pairs[size - 2].second)
            {
                idx = size - 2;
            }
            else
            {
                size_t start = 0, end = size - 1;
                while (start <= end)
                {
                    size_t mid = start + (end - start) / 2;
                    if (mid > size)
                        break;
                    if (level > value_weight_pairs[mid + 1].second)
                        start = mid + 1;
                    else
                    {
                        idx = mid;
                        end = mid - 1;
                    }
                }
            }
        }

        size_t l = idx;
        size_t u = idx + 1 < size ? idx + 1 : idx;

        Float64 xl = value_weight_pairs[l].second, xr = value_weight_pairs[u].second;
        UnderlyingType yl = value_weight_pairs[l].first, yr = value_weight_pairs[u].first;

        if (level < xl)
            yr = yl;
        if (level > xr)
            yl = yr;

        return static_cast<T>(interpolate(level, xl, xr, yl, yr));
    }

    /// Get the `size` values of `levels` quantiles. Write `size` results starting with `result` address.
    /// indices - an array of index levels such that the corresponding elements will go in ascending order.
    template <typename T>
    void getManyImpl(const Float64 * levels, const size_t * indices, size_t num_levels, Value * result) const
    {
        size_t size = map.size();

        if (0 == size)
        {
            for (size_t i = 0; i < num_levels; ++i)
                result[i] = Value();
            return;
        }

        std::vector<Pair> value_weight_pairs;
        value_weight_pairs.reserve(size);

        Float64 sum_weight = 0;
        for (const auto & pair : map)
        {
            sum_weight += pair.getMapped();
            auto value = pair.getKey();
            auto weight = pair.getMapped();
            value_weight_pairs.push_back({value, weight});
        }

        ::sort(value_weight_pairs.begin(), value_weight_pairs.end(), [](const Pair & a, const Pair & b) { return a.first < b.first; });

        Float64 accumulated = 0;

        /// vector for populating and storing the cumulative sum using the provided weights.
        /// example: [0,1,2,3,4,5] -> [0,1,3,6,10,15]
        std::vector<Float64> weights_cum_sum;
        weights_cum_sum.reserve(size);

        for (size_t idx = 0; idx < size; ++idx)
        {
            accumulated += value_weight_pairs[idx].second;
            weights_cum_sum.emplace_back(accumulated);
        }


        /// The following estimation of quantile is general and the idea is:
        /// https://en.wikipedia.org/wiki/Percentile#The_weighted_percentile_method

        /// calculates a simple cumulative distribution based on weights
        if (sum_weight != 0)
        {
            for (size_t idx = 0; idx < size; ++idx)
                value_weight_pairs[idx].second = (weights_cum_sum[idx] - 0.5 * value_weight_pairs[idx].second) / sum_weight;
        }

        for (size_t level_index = 0; level_index < num_levels; ++level_index)
        {
            /// perform linear interpolation for every level
            auto level = levels[indices[level_index]];

            size_t idx = 0;
            if (size >= 2)
            {
                if (level >= value_weight_pairs[size - 2].second)
                {
                    idx = size - 2;
                }
                else
                {
                    size_t start = 0, end = size - 1;
                    while (start <= end)
                    {
                        size_t mid = start + (end - start) / 2;
                        if (mid > size)
                            break;
                        if (level > value_weight_pairs[mid + 1].second)
                            start = mid + 1;
                        else
                        {
                            idx = mid;
                            end = mid - 1;
                        }
                    }
                }
            }

            size_t l = idx;
            size_t u = idx + 1 < size ? idx + 1 : idx;

            Float64 xl = value_weight_pairs[l].second, xr = value_weight_pairs[u].second;
            UnderlyingType yl = value_weight_pairs[l].first, yr = value_weight_pairs[u].first;

            if (level < xl)
                yr = yl;
            if (level > xr)
                yl = yr;

            result[indices[level_index]] = static_cast<T>(interpolate(level, xl, xr, yl, yr));
        }
    }

    /// This ignores overflows or NaN's that might arise during add, sub and mul operations and doesn't aim to provide exact
    /// results since `the quantileInterpolatedWeighted` function itself relies mainly on approximation.
    UnderlyingType NO_SANITIZE_UNDEFINED interpolate(Float64 level, Float64 xl, Float64 xr, UnderlyingType yl, UnderlyingType yr) const
    {
        UnderlyingType dy = yr - yl;
        Float64 dx = xr - xl;
        dx = dx == 0 ? 1 : dx; /// to handle NaN behavior that might arise during integer division below.

        /// yl + (dy / dx) * (level - xl)
        return static_cast<UnderlyingType>(yl + (dy / dx) * (level - xl));
    }
};


template <typename Value, bool _> using FuncQuantileInterpolatedWeighted = AggregateFunctionQuantile<Value, QuantileInterpolatedWeighted<Value>, NameQuantileInterpolatedWeighted, true, void, false, false>;
template <typename Value, bool _> using FuncQuantilesInterpolatedWeighted = AggregateFunctionQuantile<Value, QuantileInterpolatedWeighted<Value>, NameQuantilesInterpolatedWeighted, true, void, true, false>;

template <template <typename, bool> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires at least one argument", name);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);

#define DISPATCH(TYPE) \
if (which.idx == TypeIndex::TYPE) return std::make_shared<Function<TYPE, true>>(argument_types, params);
    FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Date) return std::make_shared<Function<DataTypeDate::FieldType, false>>(argument_types, params);
    if (which.idx == TypeIndex::DateTime) return std::make_shared<Function<DataTypeDateTime::FieldType, false>>(argument_types, params);

    if (which.idx == TypeIndex::Decimal32) return std::make_shared<Function<Decimal32, false>>(argument_types, params);
    if (which.idx == TypeIndex::Decimal64) return std::make_shared<Function<Decimal64, false>>(argument_types, params);
    if (which.idx == TypeIndex::Decimal128) return std::make_shared<Function<Decimal128, false>>(argument_types, params);
    if (which.idx == TypeIndex::Decimal256) return std::make_shared<Function<Decimal256, false>>(argument_types, params);
    if (which.idx == TypeIndex::DateTime64) return std::make_shared<Function<DateTime64, false>>(argument_types, params);

    if (which.idx == TypeIndex::Int128) return std::make_shared<Function<Int128, true>>(argument_types, params);
    if (which.idx == TypeIndex::UInt128) return std::make_shared<Function<UInt128, true>>(argument_types, params);
    if (which.idx == TypeIndex::Int256) return std::make_shared<Function<Int256, true>>(argument_types, params);
    if (which.idx == TypeIndex::UInt256) return std::make_shared<Function<UInt256, true>>(argument_types, params);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                    argument_type->getName(), name);
}

}

void registerAggregateFunctionsQuantileInterpolatedWeighted(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    factory.registerFunction(NameQuantileInterpolatedWeighted::name, createAggregateFunctionQuantile<FuncQuantileInterpolatedWeighted>);
    factory.registerFunction(NameQuantilesInterpolatedWeighted::name, { createAggregateFunctionQuantile<FuncQuantilesInterpolatedWeighted>, properties });

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianInterpolatedWeighted", NameQuantileInterpolatedWeighted::name);
}

}
