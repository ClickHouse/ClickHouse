#include <AggregateFunctions/AggregateFunctionQuantile.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Core/Field.h>
#include <cmath>
#include <Common/RadixSort.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

template <typename T>
class ApproxSampler
{
public:
    struct Stats
    {
        T value;     // The sampled value
        Int64 g;     // The minimum rank jump from the previous value's minimum rank
        Int64 delta; // The maximum span of the rank

        Stats() = default;
        Stats(T value_, Int64 g_, Int64 delta_) : value(value_), g(g_), delta(delta_) { }
    };

    struct QueryResult
    {
        size_t index;
        Int64 rank;
        T value;

        QueryResult(size_t index_, Int64 rank_, T value_) : index(index_), rank(rank_), value(value_) { }
    };

    ApproxSampler() = default;

    ApproxSampler(const ApproxSampler & other)
        : relative_error(other.relative_error)
        , compress_threshold(other.compress_threshold)
        , count(other.count)
        , compressed(other.compressed)
        , sampled(other.sampled.begin(), other.sampled.end())
        , backup_sampled(other.backup_sampled.begin(), other.backup_sampled.end())
        , head_sampled(other.head_sampled.begin(), other.head_sampled.end())
    {
    }

    explicit ApproxSampler(double relative_error_)
        : relative_error(relative_error_), compress_threshold(default_compress_threshold), count(0), compressed(false)
    {
    }

    bool isCompressed() const { return compressed; }
    void setCompressed() { compressed = true; }

    void insert(T x)
    {
        head_sampled.push_back(x);
        compressed = false;
        if (head_sampled.size() >= default_head_size)
        {
            withHeadBufferInserted();
            if (sampled.size() >= compress_threshold)
                compress();
        }
    }

    void query(const Float64 * percentiles, const size_t * indices, size_t size, T * result) const
    {
        if (!head_sampled.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot operate on an uncompressed summary, call compress() first");

        if (sampled.empty())
        {
            for (size_t i = 0; i < size; ++i)
                result[i] = T();
            return;
        }

        Int64 current_max = std::numeric_limits<Int64>::min();
        for (const auto & stats : sampled)
            current_max = std::max(stats.delta + stats.g, current_max);
        Int64 target_error = current_max / 2;

        size_t index = 0;
        auto min_rank = sampled[0].g;
        for (size_t i = 0; i < size; ++i)
        {
            double percentile = percentiles[indices[i]];
            if (percentile <= relative_error)
            {
                result[indices[i]] = sampled.front().value;
            }
            else if (percentile >= 1 - relative_error)
            {
                result[indices[i]] = sampled.back().value;
            }
            else
            {
                QueryResult res = findApproxQuantile(index, min_rank, target_error, percentile);
                index = res.index;
                min_rank = res.rank;
                result[indices[i]] = res.value;
            }
        }
    }

    void compress()
    {
        if (compressed)
            return;

        withHeadBufferInserted();

        doCompress(2 * relative_error * count);
        compressed = true;
    }


    void merge(const ApproxSampler & other)
    {
        if (other.count == 0)
            return;

        /// NOLINTBEGIN(readability-else-after-return)
        if (count == 0)
        {
            compress_threshold = other.compress_threshold;
            relative_error = other.relative_error;
            count = other.count;
            compressed = other.compressed;

            sampled.resize_exact(other.sampled.size());
            memcpy(sampled.data(), other.sampled.data(), sizeof(Stats) * other.sampled.size());
            return;
        }
        else
        {
            // Merge the two buffers.
            // The GK algorithm is a bit unclear about it, but we need to adjust the statistics during the
            // merging. The main idea is that samples that come from one side will suffer from the lack of
            // precision of the other.
            // As a concrete example, take two QuantileSummaries whose samples (value, g, delta) are:
            // `a = [(0, 1, 0), (20, 99, 0)]` and `b = [(10, 1, 0), (30, 49, 0)]`
            // This means `a` has 100 values, whose minimum is 0 and maximum is 20,
            // while `b` has 50 values, between 10 and 30.
            // The resulting samples of the merge will be:
            // a+b = [(0, 1, 0), (10, 1, ??), (20, 99, ??), (30, 49, 0)]
            // The values of `g` do not change, as they represent the minimum number of values between two
            // consecutive samples. The values of `delta` should be adjusted, however.
            // Take the case of the sample `10` from `b`. In the original stream, it could have appeared
            // right after `0` (as expressed by `g=1`) or right before `20`, so `delta=99+0-1=98`.
            // In the GK algorithm's style of working in terms of maximum bounds, one can observe that the
            // maximum additional uncertainty over samples coming from `b` is `max(g_a + delta_a) =
            // floor(2 * eps_a * n_a)`. Likewise, additional uncertainty over samples from `a` is
            // `floor(2 * eps_b * n_b)`.
            // Only samples that interleave the other side are affected. That means that samples from
            // one side that are lesser (or greater) than all samples from the other side are just copied
            // unmodified.
            // If the merging instances have different `relativeError`, the resulting instance will carry
            // the largest one: `eps_ab = max(eps_a, eps_b)`.
            // The main invariant of the GK algorithm is kept:
            // `max(g_ab + delta_ab) <= floor(2 * eps_ab * (n_a + n_b))` since
            // `max(g_ab + delta_ab) <= floor(2 * eps_a * n_a) + floor(2 * eps_b * n_b)`
            // Finally, one can see how the `insert(x)` operation can be expressed as `merge([(x, 1, 0])`
            compress();

            backup_sampled.clear();
            backup_sampled.reserve_exact(sampled.size() + other.sampled.size());
            double merged_relative_error = std::max(relative_error, other.relative_error);
            size_t merged_count = count + other.count;
            Int64 additional_self_delta = static_cast<Int64>(std::floor(2 * other.relative_error * other.count));
            Int64 additional_other_delta = static_cast<Int64>(std::floor(2 * relative_error * count));

            // Do a merge of two sorted lists until one of the lists is fully consumed
            size_t self_idx = 0;
            size_t other_idx = 0;
            while (self_idx < sampled.size() && other_idx < other.sampled.size())
            {
                const Stats & self_sample = sampled[self_idx];
                const Stats & other_sample = other.sampled[other_idx];

                // Detect next sample
                Stats next_sample;
                Int64 additional_delta = 0;
                if (self_sample.value < other_sample.value)
                {
                    ++self_idx;
                    next_sample = self_sample;
                    additional_delta = other_idx > 0 ? additional_self_delta : 0;
                }
                else
                {
                    ++other_idx;
                    next_sample = other_sample;
                    additional_delta = self_idx > 0 ? additional_other_delta : 0;
                }

                // Insert it
                next_sample.delta += additional_delta;
                backup_sampled.emplace_back(std::move(next_sample));
            }

            // Copy the remaining samples from the other list
            // (by construction, at most one `while` loop will run)
            while (self_idx < sampled.size())
            {
                backup_sampled.emplace_back(sampled[self_idx]);
                ++self_idx;
            }
            while (other_idx < other.sampled.size())
            {
                backup_sampled.emplace_back(other.sampled[other_idx]);
                ++other_idx;
            }

            std::swap(sampled, backup_sampled);
            relative_error = merged_relative_error;
            count = merged_count;
            compress_threshold = other.compress_threshold;

            doCompress(2 * merged_relative_error * merged_count);
            compressed = true;
        }
        /// NOLINTEND(readability-else-after-return)
    }

    void write(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(compress_threshold, buf);
        writeBinaryLittleEndian(relative_error, buf);
        writeBinaryLittleEndian(count, buf);
        writeBinaryLittleEndian(sampled.size(), buf);

        for (const auto & stats : sampled)
        {
            writeBinaryLittleEndian(stats.value, buf);
            writeBinaryLittleEndian(stats.g, buf);
            writeBinaryLittleEndian(stats.delta, buf);
        }
    }

    void read(ReadBuffer & buf)
    {
        readBinaryLittleEndian(compress_threshold, buf);
        if (compress_threshold != default_compress_threshold)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "The compress threshold {} isn't the expected one {}",
                compress_threshold,
                default_compress_threshold);

        readBinaryLittleEndian(relative_error, buf);
        readBinaryLittleEndian(count, buf);

        size_t sampled_len = 0;
        readBinaryLittleEndian(sampled_len, buf);
        sampled.resize_exact(sampled_len);

        for (size_t i = 0; i < sampled_len; ++i)
        {
            auto & stats = sampled[i];
            readBinaryLittleEndian(stats.value, buf);
            readBinaryLittleEndian(stats.g, buf);
            readBinaryLittleEndian(stats.delta, buf);
        }
    }

private:
    QueryResult findApproxQuantile(size_t index, Int64 min_rank_at_index, double target_error, double percentile) const
    {
        Stats curr_sample = sampled[index];
        Int64 rank = static_cast<Int64>(std::ceil(percentile * count));
        size_t i = index;
        Int64 min_rank = min_rank_at_index;
        while (i < sampled.size() - 1)
        {
            Int64 max_rank = min_rank + curr_sample.delta;
            if (max_rank - target_error <= rank && rank <= min_rank + target_error)
                return {i, min_rank, curr_sample.value};

            ++i;
            curr_sample = sampled[i];
            min_rank += curr_sample.g;
        }
        return {sampled.size() - 1, 0, sampled.back().value};
    }

    void withHeadBufferInserted()
    {
        if (head_sampled.empty())
            return;

        bool use_radix_sort = head_sampled.size() >= 256 && (is_arithmetic_v<T> && !is_big_int_v<T>);
        if (use_radix_sort)
            RadixSort<RadixSortNumTraits<T>>::executeLSD(head_sampled.data(), head_sampled.size());
        else
            ::sort(head_sampled.begin(), head_sampled.end());

        backup_sampled.clear();
        backup_sampled.reserve_exact(sampled.size() + head_sampled.size());

        size_t sample_idx = 0;
        size_t ops_idx = 0;
        size_t current_count = count;
        for (; ops_idx < head_sampled.size(); ++ops_idx)
        {
            T current_sample = head_sampled[ops_idx];

            // Add all the samples before the next observation.
            while (sample_idx < sampled.size() && sampled[sample_idx].value <= current_sample)
            {
                backup_sampled.emplace_back(sampled[sample_idx]);
                ++sample_idx;
            }

            // If it is the first one to insert, of if it is the last one
            ++current_count;
            Int64 delta;
            if (backup_sampled.empty() || (sample_idx == sampled.size() && ops_idx == (head_sampled.size() - 1)))
                delta = 0;
            else
                delta = static_cast<Int64>(std::floor(2 * relative_error * current_count));

            backup_sampled.emplace_back(current_sample, 1, delta);
        }

        // Add all the remaining existing samples
        for (; sample_idx < sampled.size(); ++sample_idx)
            backup_sampled.emplace_back(sampled[sample_idx]);

        std::swap(sampled, backup_sampled);
        head_sampled.clear();
        count = current_count;
    }


    void doCompress(double merge_threshold)
    {
        if (sampled.empty())
            return;

        backup_sampled.clear();
        // Start for the last element, which is always part of the set.
        // The head contains the current new head, that may be merged with the current element.
        Stats head = sampled.back();
        ssize_t i = sampled.size() - 2;

        // Do not compress the last element
        while (i >= 1)
        {
            // The current sample:
            const auto & sample1 = sampled[i];
            // Do we need to compress?
            if (sample1.g + head.g + head.delta < merge_threshold)
            {
                // Do not insert yet, just merge the current element into the head.
                head.g += sample1.g;
            }
            else
            {
                // Prepend the current head, and keep the current sample as target for merging.
                backup_sampled.push_back(head);
                head = sample1;
            }
            --i;
        }

        backup_sampled.push_back(head);
        // If necessary, add the minimum element:
        auto curr_head = sampled.front();

        // don't add the minimum element if `currentSamples` has only one element (both `currHead` and
        // `head` point to the same element)
        if (curr_head.value <= head.value && sampled.size() > 1)
            backup_sampled.emplace_back(sampled.front());

        std::reverse(backup_sampled.begin(), backup_sampled.end());
        std::swap(sampled, backup_sampled);
    }

    double relative_error;
    size_t compress_threshold;
    size_t count;
    bool compressed;

    PaddedPODArray<Stats> sampled;
    PaddedPODArray<Stats> backup_sampled;
    PaddedPODArray<T> head_sampled;

    static constexpr size_t default_compress_threshold = 10000;
    static constexpr size_t default_head_size = 50000;
};

template <typename Value>
class QuantileGK
{
private:
    using Data = ApproxSampler<Value>;
    Data data;

public:
    QuantileGK() = default;

    explicit QuantileGK(size_t accuracy) : data(1.0 / static_cast<double>(accuracy)) { }

    void add(const Value & x) { data.insert(x); }

    template <typename Weight>
    void add(const Value &, const Weight &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method add with weight is not implemented for GKSampler");
    }

    void merge(const QuantileGK & rhs)
    {
        if (!data.isCompressed())
            data.compress();

        if (rhs.data.isCompressed())
            data.merge(rhs.data);
        else
        {
            /// We can't modify rhs, so copy it and compress
            Data rhs_data_copy(rhs.data);
            rhs_data_copy.compress();
            data.merge(rhs_data_copy);
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        if (data.isCompressed())
            data.write(buf);
        else
        {
            /// We can't modify rhs, so copy it and compress
            Data data_copy(data);
            data_copy.compress();
            data_copy.write(buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        data.read(buf);
        /// Serialized data is always compressed
        data.setCompressed();
    }

    /// Get the value of the `level` quantile. The level must be between 0 and 1.
    Value get(Float64 level)
    {
        if (!data.isCompressed())
            data.compress();

        Value res;
        size_t indice = 0;
        data.query(&level, &indice, 1, &res);
        return res;
    }

    /// Get the `size` values of `levels` quantiles. Write `size` results starting with `result` address.
    /// indices - an array of index levels such that the corresponding elements will go in ascending order.
    void getMany(const Float64 * levels, const size_t * indices, size_t size, Value * result)
    {
        if (!data.isCompressed())
            data.compress();

        data.query(levels, indices, size, result);
    }

    Float64 getFloat64(Float64 /*level*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getFloat64 is not implemented for GKSampler");
    }

    void getManyFloat(const Float64 * /*levels*/, const size_t * /*indices*/, size_t /*size*/, Float64 * /*result*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getManyFloat is not implemented for GKSampler");
    }
};

template <typename Value, bool _> using FuncQuantileGK = AggregateFunctionQuantile<Value, QuantileGK<Value>, NameQuantileGK, false, void, false, true>;
template <typename Value, bool _> using FuncQuantilesGK = AggregateFunctionQuantile<Value, QuantileGK<Value>, NameQuantilesGK, false, void, true, true>;

template <template <typename, bool> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires at least one argument", name);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return std::make_shared<Function<TYPE, true>>(argument_types, params);
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

void registerAggregateFunctionsQuantileApprox(AggregateFunctionFactory & factory)
{
    /// For aggregate functions returning array we cannot return NULL on empty set.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };

    factory.registerFunction(NameQuantileGK::name, createAggregateFunctionQuantile<FuncQuantileGK>);
    factory.registerFunction(NameQuantilesGK::name, {createAggregateFunctionQuantile<FuncQuantilesGK>, properties});

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("medianGK", NameQuantileGK::name);
}

}
