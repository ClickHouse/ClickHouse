#pragma once

#include <cmath>
#include <Common/RadixSort.h>
#include <Common/PODArray.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
}


/** The algorithm was implemented by Alexei Borzenkov https://github.com/snaury
  * He owns the authorship of the code and half the comments in this namespace,
  * except for merging, serialization, and sorting, as well as selecting types and other changes.
  * We thank Alexei Borzenkov for writing the original code.
  */

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
template <typename T>
class QuantileTDigest
{
    using Value = Float32;
    using Count = Float32;

    /** The centroid stores the weight of points around their mean value
      */
    struct Centroid
    {
        Value mean;
        Count count;

        Centroid() = default;

        explicit Centroid(Value mean_, Count count_)
            : mean(mean_)
            , count(count_)
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
    struct Params
    {
        Value epsilon = 0.01;
        size_t max_unmerged = 2048;
    };

    Params params;

    /// The memory will be allocated to several elements at once, so that the state occupies 64 bytes.
    static constexpr size_t bytes_in_arena = 128 - sizeof(PODArray<Centroid>) - sizeof(Count) - sizeof(UInt32);
    using Summary = PODArrayWithStackMemory<Centroid, bytes_in_arena>;

    Summary summary;
    Count count = 0;
    UInt32 unmerged = 0;

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
        using Result = Element;
        using Key = Value;
        using CountType = UInt32;
        using KeyBits = UInt32;

        static constexpr size_t PART_SIZE_BITS = 8;

        using Transform = RadixSortFloatTransform<KeyBits>;
        using Allocator = RadixSortMallocAllocator;

        /// The function to get the key from an array element.
        static Key & extractKey(Element & elem) { return elem.mean; }
        static Result & extractResult(Element & elem) { return elem; }
    };

    /** Adds a centroid `c` to the digest
      */
    void addCentroid(const Centroid & c)
    {
        summary.push_back(c);
        count += c.count;
        ++unmerged;
        if (unmerged >= params.max_unmerged)
            compress();
    }

    /** Performs compression of accumulated centroids
      * When merging, the invariant is retained to the maximum size of each
      * centroid that does not exceed `4 q (1 - q) \ delta N`.
      */
    void compress()
    {
        if (unmerged > 0)
        {
            RadixSort<RadixSortTraits>::executeLSD(summary.data(), summary.size());

            if (summary.size() > 3)
            {
                /// A pair of consecutive bars of the histogram.
                auto l = summary.begin();
                auto r = std::next(l);

                Count sum = 0;
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

public:
    /** Adds to the digest a change in `x` with a weight of `cnt` (default 1)
      */
    void add(T x, UInt64 cnt = 1)
    {
        addCentroid(Centroid(Value(x), Count(cnt)));
    }

    void merge(const QuantileTDigest & other)
    {
        for (const auto & c : other.summary)
            addCentroid(c);
    }

    void serialize(WriteBuffer & buf)
    {
        compress();
        writeVarUInt(summary.size(), buf);
        buf.write(reinterpret_cast<const char *>(summary.data()), summary.size() * sizeof(summary[0]));
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (size > params.max_unmerged)
            throw Exception("Too large t-digest summary size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        summary.resize(size);
        buf.read(reinterpret_cast<char *>(summary.data()), size * sizeof(summary[0]));

        count = 0;
        for (const auto & c : summary)
            count += c.count;
    }

    /** Calculates the quantile q [0, 1] based on the digest.
      * For an empty digest returns NaN.
      */
    template <typename ResultType>
    ResultType getImpl(Float64 level)
    {
        if (summary.empty())
            return std::is_floating_point_v<ResultType> ? NAN : 0;

        compress();

        if (summary.size() == 1)
            return summary.front().mean;

        Float64 x = level * count;
        Float64 prev_x = 0;
        Count sum = 0;
        Value prev_mean = summary.front().mean;

        for (const auto & c : summary)
        {
            Float64 current_x = sum + c.count * 0.5;

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
    void getManyImpl(const Float64 * levels, const size_t * levels_permutation, size_t size, ResultType * result)
    {
        if (summary.empty())
        {
            for (size_t result_num = 0; result_num < size; ++result_num)
                result[result_num] = std::is_floating_point_v<ResultType> ? NAN : 0;
            return;
        }

        compress();

        if (summary.size() == 1)
        {
            for (size_t result_num = 0; result_num < size; ++result_num)
                result[result_num] = summary.front().mean;
            return;
        }

        Float64 x = levels[levels_permutation[0]] * count;
        Float64 prev_x = 0;
        Count sum = 0;
        Value prev_mean = summary.front().mean;

        size_t result_num = 0;
        for (const auto & c : summary)
        {
            Float64 current_x = sum + c.count * 0.5;

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

    T get(Float64 level)
    {
        return getImpl<T>(level);
    }

    Float32 getFloat(Float64 level)
    {
        return getImpl<Float32>(level);
    }

    void getMany(const Float64 * levels, const size_t * indices, size_t size, T * result)
    {
        getManyImpl(levels, indices, size, result);
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t size, Float32 * result)
    {
        getManyImpl(levels, indices, size, result);
    }
};

}
