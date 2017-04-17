#pragma once

#include <cmath>
#include <cstdint>
#include <cassert>

#include <vector>
#include <algorithm>

#include <Common/RadixSort.h>
#include <Common/PODArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <AggregateFunctions/IUnaryAggregateFunction.h>
#include <AggregateFunctions/IBinaryAggregateFunction.h>
#include <AggregateFunctions/QuantilesCommon.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
}
}

/** The algorithm was implemented by Alexei Borzenkov https: //███████████.yandex-team.ru/snaury
  * He owns the authorship of the code and half the comments in this namespace,
  * except for merging, serialization, and sorting, as well as selecting types and other changes.
  * We thank Alexei Borzenkov for writing the original code.
  */
namespace tdigest
{

/**
  * The centroid stores the weight of points around their mean value
  */
template <typename Value, typename Count>
struct Centroid
{
    Value mean;
    Count count;

    Centroid() = default;

    explicit Centroid(Value mean, Count count = 1)
        : mean(mean)
        , count(count)
    {}

    Centroid & operator+=(const Centroid & other)
    {
        count += other.count;
        mean += other.count * (other.mean - mean) / count;
        return *this;
    }

    bool operator<(const Centroid & other) const
    {
        return mean < other.mean;
    }
};


/** :param epsilon: value \delta from the article - error in the range
  *                    quantile 0.5 (default is 0.01, i.e. 1%)
  * :param max_unmerged: when accumulating count of new points beyond this
  *                      value centroid compression is triggered
  *                      (default is 2048, the higher the value - the
  *                      more memory is required, but amortization of execution time increases)
  */
template <typename Value>
struct Params
{
    Value epsilon = 0.01;
    size_t max_unmerged = 2048;
};


/** Implementation of t-digest algorithm (https://github.com/tdunning/t-digest).
  * This option is very similar to MergingDigest on java, however the decision about
  * the union is accepted based on the original condition from the article
  * (via a size constraint, using the approximation of the quantile of each
  * centroid, not the distance on the curve of the position of their boundaries). MergingDigest
  * on java gives significantly fewer centroids than this variant, that
  * negatively affects accuracy with the same compression factor, but gives
  * size guarantees. The author himself on the proposal for this variant said that
  * the size of the digest grows like O(log(n)), while the version on java
  * does not depend on the expected number of points. Also an variant on java
  * uses asin, which slows down the algorithm a bit.
  */
template <typename Value, typename CentroidCount, typename TotalCount>
class MergingDigest
{
    using Params = tdigest::Params<Value>;
    using Centroid = tdigest::Centroid<Value, CentroidCount>;

    /// The memory will be allocated to several elements at once, so that the state occupies 64 bytes.
    static constexpr size_t bytes_in_arena = 64 - sizeof(DB::PODArray<Centroid>) - sizeof(TotalCount) - sizeof(uint32_t);

    using Summary = DB::PODArray<Centroid, bytes_in_arena / sizeof(Centroid), AllocatorWithStackMemory<Allocator<false>, bytes_in_arena>>;

    Summary summary;
    TotalCount count = 0;
    uint32_t unmerged = 0;

    /** Linear interpolation at the point x on the line (x1, y1)..(x2, y2)
      */
    static Value interpolate(Value x, Value x1, Value y1, Value x2, Value y2)
    {
        double k = (x - x1) / (x2 - x1);
        return y1 + k * (y2 - y1);
    }

    struct RadixSortTraits
    {
        using Element = Centroid;
        using Key = Value;
        using CountType = uint32_t;
        using KeyBits = uint32_t;

        static constexpr size_t PART_SIZE_BITS = 8;

        using Transform = RadixSortFloatTransform<KeyBits>;
        using Allocator = RadixSortMallocAllocator;

        /// The function to get the key from an array element.
        static Key & extractKey(Element & elem) { return elem.mean; }
    };

public:
    /** Adds to the digest a change in `x` with a weight of `cnt` (default 1)
      */
    void add(const Params & params, Value x, CentroidCount cnt = 1)
    {
        add(params, Centroid(x, cnt));
    }

    /** Adds a centroid `c` to the digest
      */
    void add(const Params & params, const Centroid & c)
    {
        summary.push_back(c);
        count += c.count;
        ++unmerged;
        if (unmerged >= params.max_unmerged)
            compress(params);
    }

    /** Performs compression of accumulated centroids
      * When merging, the invariant is retained to the maximum size of each
      * centroid that does not exceed `4 q (1 - q) \ delta N`.
      */
    void compress(const Params & params)
    {
        if (unmerged > 0)
        {
            RadixSort<RadixSortTraits>::execute(&summary[0], summary.size());

            if (summary.size() > 3)
            {
        /// A pair of consecutive bars of the histogram.
                auto l = summary.begin();
                auto r = std::next(l);

                TotalCount sum = 0;
                while (r != summary.end())
                {
                    // we use quantile which gives us the smallest error

        /// The ratio of the part of the histogram to l, including the half l to the entire histogram. That is, what level quantile in position l.
                    Value ql = (sum + l->count * 0.5) / count;
                    Value err = ql * (1 - ql);

        /// The ratio of the portion of the histogram to l, including l and half r to the entire histogram. That is, what level is the quantile in position r.
                    Value qr = (sum + l->count + r->count * 0.5) / count;
                    Value err2 = qr * (1 - qr);

                    if (err > err2)
                        err = err2;

                    Value k = 4 * count * err * params.epsilon;

        /** The ratio of the weight of the glued column pair to all values is not greater,
          *  than epsilon multiply by a certain quadratic coefficient, which in the median is 1 (4 * 1/2 * 1/2),
          *  and at the edges decreases and is approximately equal to the distance to the edge * 4.
                      */

                    if (l->count + r->count <= k)
                    {
                        // it is possible to merge left and right
        /// The left column "eats" the right.
                        *l += *r;
                    }
                    else
                    {
                        // not enough capacity, check the next pair
                        sum += l->count;
                        ++l;

        /// We skip all the values "eaten" earlier.
                        if (l != r)
                            *l = *r;
                    }
                    ++r;
                }

        /// At the end of the loop, all values to the right of l were "eaten".
                summary.resize(l - summary.begin() + 1);
            }

            unmerged = 0;
        }
    }

    /** Calculates the quantile q [0, 1] based on the digest.
      * For an empty digest returns NaN.
      */
    Value getQuantile(const Params & params, Value q)
    {
        if (summary.empty())
            return NAN;

        compress(params);

        if (summary.size() == 1)
            return summary.front().mean;

        Value x = q * count;
        TotalCount sum = 0;
        Value prev_mean = summary.front().mean;
        Value prev_x = 0;

        for (const auto & c : summary)
        {
            Value current_x = sum + c.count * 0.5;

            if (current_x >= x)
                return interpolate(x, prev_x, prev_mean, current_x, c.mean);

            sum += c.count;
            prev_mean = c.mean;
            prev_x = current_x;
        }

        return summary.back().mean;
    }

    /** Get multiple quantiles (`size` parts).
      * levels - an array of levels of the desired quantiles. They are in a random order.
      * levels_permutation - array-permutation levels. The i-th position will be the index of the i-th ascending level in the `levels` array.
      * result - the array where the results are added, in order of `levels`,
      */
    template <typename ResultType>
    void getManyQuantiles(const Params & params, const Value * levels, const size_t * levels_permutation, size_t size, ResultType * result)
    {
        if (summary.empty())
        {
            for (size_t result_num = 0; result_num < size; ++result_num)
                result[result_num] = std::is_floating_point<ResultType>::value ? NAN : 0;
            return;
        }

        compress(params);

        if (summary.size() == 1)
        {
            for (size_t result_num = 0; result_num < size; ++result_num)
                result[result_num] = summary.front().mean;
            return;
        }

        Value x = levels[levels_permutation[0]] * count;
        TotalCount sum = 0;
        Value prev_mean = summary.front().mean;
        Value prev_x = 0;

        size_t result_num = 0;
        for (const auto & c : summary)
        {
            Value current_x = sum + c.count * 0.5;

            while (current_x >= x)
            {
                result[levels_permutation[result_num]] = interpolate(x, prev_x, prev_mean, current_x, c.mean);

                ++result_num;
                if (result_num >= size)
                    return;

                x = levels[levels_permutation[result_num]] * count;
            }

            sum += c.count;
            prev_mean = c.mean;
            prev_x = current_x;
        }

        auto rest_of_results = summary.back().mean;
        for (; result_num < size; ++result_num)
            result[levels_permutation[result_num]] = rest_of_results;
    }

    /** Combine with another state.
      */
    void merge(const Params & params, const MergingDigest & other)
    {
        for (const auto & c : other.summary)
            add(params, c);
    }

    /** Write to the stream.
      */
    void write(const Params & params, DB::WriteBuffer & buf)
    {
        compress(params);
        DB::writeVarUInt(summary.size(), buf);
        buf.write(reinterpret_cast<const char *>(&summary[0]), summary.size() * sizeof(summary[0]));
    }

    /** Read from the stream.
      */
    void read(const Params & params, DB::ReadBuffer & buf)
    {
        size_t size = 0;
        DB::readVarUInt(size, buf);

        if (size > params.max_unmerged)
            throw DB::Exception("Too large t-digest summary size", DB::ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        summary.resize(size);
        buf.read(reinterpret_cast<char *>(&summary[0]), size * sizeof(summary[0]));
    }
};

}


namespace DB
{

struct AggregateFunctionQuantileTDigestData
{
    tdigest::MergingDigest<Float32, Float32, Float32> digest;
};


template <typename T, bool returns_float = true>
class AggregateFunctionQuantileTDigest final
    : public IUnaryAggregateFunction<AggregateFunctionQuantileTDigestData, AggregateFunctionQuantileTDigest<T>>
{
private:
    Float32 level;
    tdigest::Params<Float32> params;
    DataTypePtr type;

public:
    AggregateFunctionQuantileTDigest(double level_ = 0.5) : level(level_) {}

    String getName() const override { return "quantileTDigest"; }

    DataTypePtr getReturnType() const override
    {
        return type;
    }

    void setArgument(const DataTypePtr & argument)
    {
        if (returns_float)
            type = std::make_shared<DataTypeFloat32>();
        else
            type = argument;
    }

    void setParameters(const Array & params) override
    {
        if (params.size() != 1)
            throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        level = applyVisitor(FieldVisitorConvertToNumber<Float32>(), params[0]);
    }

    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena *) const
    {
        this->data(place).digest.add(params, static_cast<const ColumnVector<T> &>(column).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).digest.merge(params, this->data(rhs).digest);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(const_cast<AggregateDataPtr>(place)).digest.write(params, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).digest.read(params, buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto quantile = this->data(const_cast<AggregateDataPtr>(place)).digest.getQuantile(params, level);

        if (returns_float)
            static_cast<ColumnFloat32 &>(to).getData().push_back(quantile);
        else
            static_cast<ColumnVector<T> &>(to).getData().push_back(quantile);
    }
};


template <typename T, typename Weight, bool returns_float = true>
class AggregateFunctionQuantileTDigestWeighted final
    : public IBinaryAggregateFunction<AggregateFunctionQuantileTDigestData, AggregateFunctionQuantileTDigestWeighted<T, Weight, returns_float>>
{
private:
    Float32 level;
    tdigest::Params<Float32> params;
    DataTypePtr type;

public:
    AggregateFunctionQuantileTDigestWeighted(double level_ = 0.5) : level(level_) {}

    String getName() const override { return "quantileTDigestWeighted"; }

    DataTypePtr getReturnType() const override
    {
        return type;
    }

    void setArgumentsImpl(const DataTypes & arguments)
    {
        if (returns_float)
            type = std::make_shared<DataTypeFloat32>();
        else
            type = arguments.at(0);
    }

    void setParameters(const Array & params) override
    {
        if (params.size() != 1)
            throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        level = applyVisitor(FieldVisitorConvertToNumber<Float32>(), params[0]);
    }

    void addImpl(AggregateDataPtr place, const IColumn & column_value, const IColumn & column_weight, size_t row_num, Arena *) const
    {
        this->data(place).digest.add(params,
            static_cast<const ColumnVector<T> &>(column_value).getData()[row_num],
            static_cast<const ColumnVector<Weight> &>(column_weight).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).digest.merge(params, this->data(rhs).digest);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(const_cast<AggregateDataPtr>(place)).digest.write(params, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).digest.read(params, buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto quantile = this->data(const_cast<AggregateDataPtr>(place)).digest.getQuantile(params, level);

        if (returns_float)
            static_cast<ColumnFloat32 &>(to).getData().push_back(quantile);
        else
            static_cast<ColumnVector<T> &>(to).getData().push_back(quantile);
    }
};


template <typename T, bool returns_float = true>
class AggregateFunctionQuantilesTDigest final
    : public IUnaryAggregateFunction<AggregateFunctionQuantileTDigestData, AggregateFunctionQuantilesTDigest<T>>
{
private:
    QuantileLevels<Float32> levels;
    tdigest::Params<Float32> params;
    DataTypePtr type;

public:
    String getName() const override { return "quantilesTDigest"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(type);
    }

    void setArgument(const DataTypePtr & argument)
    {
        if (returns_float)
            type = std::make_shared<DataTypeFloat32>();
        else
            type = argument;
    }

    void setParameters(const Array & params) override
    {
        levels.set(params);
    }

    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena *) const
    {
        this->data(place).digest.add(params, static_cast<const ColumnVector<T> &>(column).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).digest.merge(params, this->data(rhs).digest);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(const_cast<AggregateDataPtr>(place)).digest.write(params, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).digest.read(params, buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();

        size_t size = levels.size();
        offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

        if (!size)
            return;

        if (returns_float)
        {
            typename ColumnFloat32::Container_t & data_to = static_cast<ColumnFloat32 &>(arr_to.getData()).getData();
            size_t old_size = data_to.size();
            data_to.resize(data_to.size() + size);

            this->data(const_cast<AggregateDataPtr>(place)).digest.getManyQuantiles(
                params, &levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
        }
        else
        {
            typename ColumnVector<T>::Container_t & data_to = static_cast<ColumnVector<T> &>(arr_to.getData()).getData();
            size_t old_size = data_to.size();
            data_to.resize(data_to.size() + size);

            this->data(const_cast<AggregateDataPtr>(place)).digest.getManyQuantiles(
                params, &levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
        }
    }
};


template <typename T, typename Weight, bool returns_float = true>
class AggregateFunctionQuantilesTDigestWeighted final
    : public IBinaryAggregateFunction<AggregateFunctionQuantileTDigestData, AggregateFunctionQuantilesTDigestWeighted<T, Weight, returns_float>>
{
private:
    QuantileLevels<Float32> levels;
    tdigest::Params<Float32> params;
    DataTypePtr type;

public:
    String getName() const override { return "quantilesTDigest"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(type);
    }

    void setArgumentsImpl(const DataTypes & arguments)
    {
        if (returns_float)
            type = std::make_shared<DataTypeFloat32>();
        else
            type = arguments.at(0);
    }

    void setParameters(const Array & params) override
    {
        levels.set(params);
    }

    void addImpl(AggregateDataPtr place, const IColumn & column_value, const IColumn & column_weight, size_t row_num, Arena *) const
    {
        this->data(place).digest.add(params,
            static_cast<const ColumnVector<T> &>(column_value).getData()[row_num],
            static_cast<const ColumnVector<Weight> &>(column_weight).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).digest.merge(params, this->data(rhs).digest);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(const_cast<AggregateDataPtr>(place)).digest.write(params, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).digest.read(params, buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();

        size_t size = levels.size();
        offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

        if (!size)
            return;

        if (returns_float)
        {
            typename ColumnFloat32::Container_t & data_to = static_cast<ColumnFloat32 &>(arr_to.getData()).getData();
            size_t old_size = data_to.size();
            data_to.resize(data_to.size() + size);

            this->data(const_cast<AggregateDataPtr>(place)).digest.getManyQuantiles(
                params, &levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
        }
        else
        {
            typename ColumnVector<T>::Container_t & data_to = static_cast<ColumnVector<T> &>(arr_to.getData()).getData();
            size_t old_size = data_to.size();
            data_to.resize(data_to.size() + size);

            this->data(const_cast<AggregateDataPtr>(place)).digest.getManyQuantiles(
                params, &levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
        }
    }
};

}
