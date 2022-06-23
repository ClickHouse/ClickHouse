#pragma once

#include <cmath>
#include <Common/RadixSort.h>
#include <Common/PODArray.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/VarInt.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
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
    using BetterFloat = Float64; // For intermediate results and sum(Count). Must have better precision, than Count

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

        bool operator<(const Centroid & other) const
        {
            return mean < other.mean;
        }
    };


    /** :param epsilon: value \delta from the article - error in the range
      *                    quantile 0.5 (default is 0.01, i.e. 1%)
      *                    if you change epsilon, you must also change max_centroids
      * :param max_centroids: depends on epsilon, the better accuracy, the more centroids you need
      *                       to describe data with this accuracy. Read article before changing.
      * :param max_unmerged: when accumulating count of new points beyond this
      *                      value centroid compression is triggered
      *                      (default is 2048, the higher the value - the
      *                      more memory is required, but amortization of execution time increases)
      *                      Change freely anytime.
      */
    struct Params
    {
        Value epsilon = 0.01;
        size_t max_centroids = 2048;
        size_t max_unmerged = 2048;
    };
    /** max_centroids_deserialize should be >= all max_centroids ever used in production.
     *  This is security parameter, preventing allocation of too much centroids in deserialize, so can be relatively large.
     */
    static constexpr size_t max_centroids_deserialize = 65536;

    static constexpr Params params{};

    static constexpr size_t bytes_in_arena = 128 - sizeof(PODArray<Centroid>) - sizeof(BetterFloat) - sizeof(size_t); // If alignment is imperfect, sizeof(TDigest) will be more than naively expected
    using Centroids = PODArrayWithStackMemory<Centroid, bytes_in_arena>;

    Centroids centroids;
    BetterFloat count = 0;
    size_t unmerged = 0;

    /** Linear interpolation at the point x on the line (x1, y1)..(x2, y2)
      */
    static Value interpolate(Value x, Value x1, Value y1, Value x2, Value y2)
    {
        /// Symmetric interpolation for better results with infinities.
        double k = (x - x1) / (x2 - x1);
        return (1 - k) * y1 + k * y2;
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
        using Allocator = RadixSortAllocator;

        /// The function to get the key from an array element.
        static Key & extractKey(Element & elem) { return elem.mean; }
        static Result & extractResult(Element & elem) { return elem; }
    };

    /** Adds a centroid `c` to the digest
     * centroid must be valid, validity is checked in add(), deserialize() and is maintained by compress()
      */
    void addCentroid(const Centroid & c)
    {
        centroids.push_back(c);
        count += c.count;
        ++unmerged;
        if (unmerged > params.max_unmerged)
            compress();
    }

    inline bool canBeMerged(const BetterFloat & l_mean, const Value & r_mean)
    {
        return l_mean == r_mean || (!std::isinf(l_mean) && !std::isinf(r_mean));
    }

    void compressBrute()
    {
        if (centroids.size() <= params.max_centroids)
            return;
        const size_t batch_size = (centroids.size() + params.max_centroids - 1) / params.max_centroids; // at least 2

        auto l = centroids.begin();
        auto r = std::next(l);
        BetterFloat sum = 0;
        BetterFloat l_mean = l->mean; // We have high-precision temporaries for numeric stability
        BetterFloat l_count = l->count;
        size_t batch_pos = 0;

        for (; r != centroids.end(); ++r)
        {
            if (batch_pos < batch_size - 1)
            {
                /// The left column "eats" the right. Middle of the batch
                l_count += r->count;
                if (r->mean != l_mean) /// Handling infinities of the same sign well.
                {
                    l_mean += r->count * (r->mean - l_mean) / l_count; // Symmetric algo (M1*C1 + M2*C2)/(C1+C2) is numerically better, but slower
                }
                l->mean = l_mean;
                l->count = l_count;
                batch_pos += 1;
            }
            else
            {
                // End of the batch, start the next one
                if (!std::isnan(l->mean)) /// Skip writing batch result if we compressed something to nan.
                {
                    sum += l->count; // Not l_count, otherwise actual sum of elements will be different
                    ++l;
                }

                /// We skip all the values "eaten" earlier.
                *l = *r;
                l_mean = l->mean;
                l_count = l->count;
                batch_pos = 0;
            }
        }

        if (!std::isnan(l->mean))
        {
            count = sum + l_count; // Update count, it might be different due to += inaccuracy
            centroids.resize(l - centroids.begin() + 1);
        }
        else /// Skip writing last batch if (super unlikely) it's nan.
        {
            count = sum;
            centroids.resize(l - centroids.begin());
        }
        // Here centroids.size() <= params.max_centroids
    }

public:
    /** Performs compression of accumulated centroids
      * When merging, the invariant is retained to the maximum size of each
      * centroid that does not exceed `4 q (1 - q) \ delta N`.
      */
    void compress()
    {
        if (unmerged > 0 || centroids.size() > params.max_centroids)
        {
            // unmerged > 0 implies centroids.size() > 0, hence *l is valid below
            RadixSort<RadixSortTraits>::executeLSD(centroids.data(), centroids.size());

            /// A pair of consecutive bars of the histogram.
            auto l = centroids.begin();
            auto r = std::next(l);

            const BetterFloat count_epsilon_4 = count * params.epsilon * 4; // Compiler is unable to do this optimization
            BetterFloat sum = 0;
            BetterFloat l_mean = l->mean; // We have high-precision temporaries for numeric stability
            BetterFloat l_count = l->count;
            while (r != centroids.end())
            {
                /// N.B. We cannot merge all the same values into single centroids because this will lead to
                /// unbalanced compression and wrong results.
                /// For more information see: https://arxiv.org/abs/1902.04023

                /// The ratio of the part of the histogram to l, including the half l to the entire histogram. That is, what level quantile in position l.
                BetterFloat ql = (sum + l_count * 0.5) / count;
                BetterFloat err = ql * (1 - ql);

                /// The ratio of the portion of the histogram to l, including l and half r to the entire histogram. That is, what level is the quantile in position r.
                BetterFloat qr = (sum + l_count + r->count * 0.5) / count;
                BetterFloat err2 = qr * (1 - qr);

                if (err > err2)
                    err = err2;

                BetterFloat k = count_epsilon_4 * err;

                /** The ratio of the weight of the glued column pair to all values is not greater,
                  *  than epsilon multiply by a certain quadratic coefficient, which in the median is 1 (4 * 1/2 * 1/2),
                  *  and at the edges decreases and is approximately equal to the distance to the edge * 4.
                  */

                if (l_count + r->count <= k && canBeMerged(l_mean, r->mean))
                {
                    // it is possible to merge left and right
                    /// The left column "eats" the right.
                    l_count += r->count;
                    if (r->mean != l_mean) /// Handling infinities of the same sign well.
                    {
                        l_mean += r->count * (r->mean - l_mean) / l_count; // Symmetric algo (M1*C1 + M2*C2)/(C1+C2) is numerically better, but slower
                    }
                    l->mean = l_mean;
                    l->count = l_count;
                }
                else
                {
                    // not enough capacity, check the next pair
                    sum += l->count; // Not l_count, otherwise actual sum of elements will be different
                    ++l;

                    /// We skip all the values "eaten" earlier.
                    if (l != r)
                        *l = *r;
                    l_mean = l->mean;
                    l_count = l->count;
                }
                ++r;
            }
            count = sum + l_count; // Update count, it might be different due to += inaccuracy

            /// At the end of the loop, all values to the right of l were "eaten".
            centroids.resize(l - centroids.begin() + 1);
            unmerged = 0;
        }

        // Ensures centroids.size() < max_centroids, independent of unprovable floating point blackbox above
        compressBrute();
    }

    /** Adds to the digest a change in `x` with a weight of `cnt` (default 1)
      */
    void add(T x, UInt64 cnt = 1)
    {
        auto vx = static_cast<Value>(x);
        if (cnt == 0 || std::isnan(vx))
            return; // Count 0 breaks compress() assumptions, Nan breaks sort(). We treat them as no sample.
        addCentroid(Centroid{vx, static_cast<Count>(cnt)});
    }

    void merge(const QuantileTDigest & other)
    {
        for (const auto & c : other.centroids)
            addCentroid(c);
    }

    void serialize(WriteBuffer & buf)
    {
        compress();
        writeVarUInt(centroids.size(), buf);
        buf.write(reinterpret_cast<const char *>(centroids.data()), centroids.size() * sizeof(centroids[0]));
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (size > max_centroids_deserialize)
            throw Exception("Too large t-digest centroids size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        count = 0;
        unmerged = 0;

        centroids.resize(size);
        // From now, TDigest will be in invalid state if exception is thrown.
        buf.read(reinterpret_cast<char *>(centroids.data()), size * sizeof(centroids[0]));

        for (const auto & c : centroids)
        {
            if (c.count <= 0 || std::isnan(c.count)) // invalid count breaks compress()
                throw Exception("Invalid centroid " + std::to_string(c.count) + ":" + std::to_string(c.mean), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
            if (!std::isnan(c.mean))
            {
                count += c.count;
            }
        }

        auto it = std::remove_if(centroids.begin(), centroids.end(), [](Centroid & c) { return std::isnan(c.mean); });
        centroids.erase(it, centroids.end());

        compress(); // Allows reading/writing TDigests with different epsilon/max_centroids params
    }

    /** Calculates the quantile q [0, 1] based on the digest.
      * For an empty digest returns NaN.
      */
    template <typename ResultType>
    ResultType getImpl(Float64 level)
    {
        if (centroids.empty())
            return std::is_floating_point_v<ResultType> ? std::numeric_limits<ResultType>::quiet_NaN() : 0;

        compress();

        if (centroids.size() == 1)
            return centroids.front().mean;

        Float64 x = level * count;
        Float64 prev_x = 0;
        Count sum = 0;
        Value prev_mean = centroids.front().mean;
        Count prev_count = centroids.front().count;

        for (const auto & c : centroids)
        {
            Float64 current_x = sum + c.count * 0.5;

            if (current_x >= x)
            {
                /// Special handling of singletons.
                Float64 left = prev_x + 0.5 * (prev_count == 1);
                Float64 right = current_x - 0.5 * (c.count == 1);

                if (x <= left)
                    return prev_mean;
                else if (x >= right)
                    return c.mean;
                else
                    return interpolate(x, left, prev_mean, right, c.mean);
            }

            sum += c.count;
            prev_mean = c.mean;
            prev_count = c.count;
            prev_x = current_x;
        }

        return centroids.back().mean;
    }

    /** Get multiple quantiles (`size` parts).
      * levels - an array of levels of the desired quantiles. They are in a random order.
      * levels_permutation - array-permutation levels. The i-th position will be the index of the i-th ascending level in the `levels` array.
      * result - the array where the results are added, in order of `levels`,
      */
    template <typename ResultType>
    void getManyImpl(const Float64 * levels, const size_t * levels_permutation, size_t size, ResultType * result)
    {
        if (centroids.empty())
        {
            for (size_t result_num = 0; result_num < size; ++result_num)
                result[result_num] = std::is_floating_point_v<ResultType> ? NAN : 0;
            return;
        }

        compress();

        if (centroids.size() == 1)
        {
            for (size_t result_num = 0; result_num < size; ++result_num)
                result[result_num] = centroids.front().mean;
            return;
        }

        Float64 x = levels[levels_permutation[0]] * count;
        Float64 prev_x = 0;
        Count sum = 0;
        Value prev_mean = centroids.front().mean;
        Count prev_count = centroids.front().count;

        size_t result_num = 0;
        for (const auto & c : centroids)
        {
            Float64 current_x = sum + c.count * 0.5;

            if (current_x >= x)
            {
                /// Special handling of singletons.
                Float64 left = prev_x + 0.5 * (prev_count == 1);
                Float64 right = current_x - 0.5 * (c.count == 1);

                while (current_x >= x)
                {
                    if (x <= left)
                        result[levels_permutation[result_num]] = prev_mean;
                    else if (x >= right)
                        result[levels_permutation[result_num]] = c.mean;
                    else
                        result[levels_permutation[result_num]] = interpolate(x, left, prev_mean, right, c.mean);

                    ++result_num;
                    if (result_num >= size)
                        return;

                    x = levels[levels_permutation[result_num]] * count;
                }
            }

            sum += c.count;
            prev_mean = c.mean;
            prev_count = c.count;
            prev_x = current_x;
        }

        auto rest_of_results = centroids.back().mean;
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
