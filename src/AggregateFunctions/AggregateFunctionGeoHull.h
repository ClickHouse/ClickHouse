#pragma once

#include <base/defines.h>

#include <boost/geometry/core/access.hpp>
#include <boost/multiprecision/cpp_int.hpp>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <iterator>
#include <limits>


namespace DB
{

/// A filtered orientation predicate for finite binary64 coordinates. `Boost.Geometry`'s
/// floating-point expansion can return zero before refinement when both products underflow.
/// Integer refinement also covers overflowing differences and mixed coordinate scales.
struct GeoHullSide
{
private:
    /// Every finite binary64 value is an integer multiple of 2^-1074 with magnitude < 2^2098.
    /// Coordinate differences need at most 2099 bits; their determinant needs at most 4199.
    /// Fixed storage avoids allocating memory while refining a predicate.
    using ExactInteger = boost::multiprecision::number<boost::multiprecision::cpp_int_backend<
        4224, 4224, boost::multiprecision::signed_magnitude, boost::multiprecision::unchecked, void>>;

    static_assert(std::numeric_limits<double>::is_iec559 && std::numeric_limits<double>::digits == 53);

    NO_INLINE static int exactSign(const std::array<double, 6> & coordinates)
    {
        std::array<uint64_t, 6> mantissas; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - filled by the loop below before read
        std::array<unsigned, 6> exponents; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - filled by the loop below before read
        unsigned common_exponent = 2046;
        unsigned largest_exponent = 0;
        for (size_t i = 0; i < coordinates.size(); ++i)
        {
            const auto bits = std::bit_cast<uint64_t>(coordinates[i]);
            const unsigned exponent = (bits >> 52) & 0x7ff;
            mantissas[i] = (bits & ((uint64_t{1} << 52) - 1)) | (exponent ? uint64_t{1} << 52 : 0);
            exponents[i] = exponent ? exponent - 1 : 0;
            if (mantissas[i])
            {
                common_exponent = std::min(common_exponent, exponents[i]);
                largest_exponent = std::max(largest_exponent, exponents[i]);
            }
        }

        /// Nearby binary exponents fit an exact determinant in native integer arithmetic.
        /// With an exponent spread <= 9, scaled coordinates have magnitude < 2^62,
        /// differences < 2^63, and the determinant < 2^127. This covers ordinary-scale
        /// cancellation without paying for the full binary64-range integer backend.
        if (largest_exponent - common_exponent <= 9)
        {
            auto narrow_integer = [&](size_t i) -> int64_t
            {
                if (!mantissas[i])
                    return 0;
                const auto value = static_cast<int64_t>(mantissas[i] << (exponents[i] - common_exponent));
                return std::signbit(coordinates[i]) ? -value : value;
            };

            const __int128_t dx1 = static_cast<__int128_t>(narrow_integer(0)) - narrow_integer(4);
            const __int128_t dy1 = static_cast<__int128_t>(narrow_integer(1)) - narrow_integer(5);
            const __int128_t dx2 = static_cast<__int128_t>(narrow_integer(2)) - narrow_integer(4);
            const __int128_t dy2 = static_cast<__int128_t>(narrow_integer(3)) - narrow_integer(5);
            const __int128_t determinant = dx1 * dy2 - dy1 * dx2;
            return determinant > 0 ? 1 : determinant < 0 ? -1 : 0;
        }

        auto integer = [&](size_t i) -> ExactInteger
        {
            if (!mantissas[i])
                return 0;
            ExactInteger value = mantissas[i];
            value <<= exponents[i] - common_exponent;
            return std::signbit(coordinates[i]) ? -value : value;
        };

        const ExactInteger dx1 = integer(0) - integer(4);
        const ExactInteger dy1 = integer(1) - integer(5);
        const ExactInteger dx2 = integer(2) - integer(4);
        const ExactInteger dy2 = integer(3) - integer(5);
        const ExactInteger determinant = dx1 * dy2 - dy1 * dx2;
        return determinant > 0 ? 1 : determinant < 0 ? -1 : 0;
    }

public:
    template <typename Point1, typename Point2, typename Point3>
    static int apply(const Point1 & first, const Point2 & second, const Point3 & third)
    {
        const double ax = boost::geometry::get<0>(first);
        const double ay = boost::geometry::get<1>(first);
        const double bx = boost::geometry::get<0>(second);
        const double by = boost::geometry::get<1>(second);
        const double cx = boost::geometry::get<0>(third);
        const double cy = boost::geometry::get<1>(third);

        if ((ax == bx && ay == by) || (ax == cx && ay == cy) || (bx == cx && by == cy)
            || (ax == bx && bx == cx) || (ay == by && by == cy))
            return 0;

        const double left = (ax - cx) * (by - cy);
        const double right = (ay - cy) * (bx - cx);
        const double determinant = left - right;
        const double magnitude = std::abs(left) + std::abs(right);

        /// A conservative error bound for the two rounded differences/products and subtraction.
        /// Keeping the bound itself normal also covers absolute errors from underflowed products.
        /// Overflow, cancellation, and smaller magnitudes are resolved exactly below.
        if (std::isfinite(magnitude) && magnitude >= 0x1p-968 && std::abs(determinant) > magnitude * 0x1p-48)
            return determinant > 0 ? 1 : -1;

        return exactSign({ax, ay, bx, by, cx, cy});
    }
};

/// Graham-Andrew scan with the same clockwise closed-ring convention as `Boost.Geometry`.
/// Partition buffers use the input's allocator and are compacted in place, so the hull's
/// workspace follows the throwing memory tracker instead of Boost's default-allocated vectors.
template <bool HasAdditional, typename MultiPoint, typename Ring>
void computeGeoConvexHullImpl(const MultiPoint & points, const MultiPoint * additional, Ring & result)
{
    result.clear();
    if (points.empty())
        return;

    auto less = [](const auto & first, const auto & second)
    {
        const auto ax = boost::geometry::get<0>(first);
        const auto bx = boost::geometry::get<0>(second);
        return ax < bx || (ax == bx && boost::geometry::get<1>(first) < boost::geometry::get<1>(second));
    };
    const auto [left_it, right_it] = std::minmax_element(points.begin(), points.end(), less);
    auto left = *left_it;
    auto right = *right_it;
    if constexpr (HasAdditional)
    {
        const auto [additional_left, additional_right] = std::minmax_element(additional->begin(), additional->end(), less);
        if (less(*additional_left, left))
            left = *additional_left;
        if (!less(*additional_right, right))
            right = *additional_right;
    }

    MultiPoint lower;
    MultiPoint upper;
    lower.push_back(left);
    upper.push_back(left);
    auto partition_points = [&](const MultiPoint & input)
    {
        for (const auto & point : input)
        {
            const int side = GeoHullSide::apply(left, right, point);
            if (side < 0)
                lower.push_back(point);
            else if (side > 0)
                upper.push_back(point);
        }
    };
    partition_points(points);
    if constexpr (HasAdditional)
        partition_points(*additional);
    lower.push_back(right);
    upper.push_back(right);

    auto scan = [&](MultiPoint & partition, int orientation)
    {
        std::sort(partition.begin(), partition.end(), less);
        size_t count = 0;
        for (size_t i = 0; i < partition.size(); ++i)
        {
            const auto point = partition[i];
            while (count >= 2 && orientation * GeoHullSide::apply(partition[count - 2], partition[count - 1], point) <= 0)
                --count;
            partition[count++] = point;
        }
        partition.resize(count);
    };
    scan(lower, 1);
    scan(upper, -1);

    result.reserve(upper.size() + lower.size());
    result.insert(result.end(), upper.begin(), upper.end());
    /// The right endpoint is already present, and the left endpoint closes the ring.
    result.insert(result.end(), std::next(lower.rbegin()), lower.rend());
    while (result.size() < 4)
        result.push_back(left);
}

template <typename MultiPoint, typename Ring>
void computeGeoConvexHull(const MultiPoint & points, Ring & result)
{
    computeGeoConvexHullImpl<false>(points, static_cast<const MultiPoint *>(nullptr), result);
}

/// Read both inputs without allocating a concatenated accumulator. The partition buffers
/// still use the tracked allocator of `MultiPoint`.
template <typename MultiPoint, typename Ring>
void computeGeoConvexHull(const MultiPoint & first, const MultiPoint & second, Ring & result)
{
    if (first.empty())
        computeGeoConvexHull(second, result);
    else if (second.empty())
        computeGeoConvexHull(first, result);
    else
        computeGeoConvexHullImpl<true>(first, &second, result);
}

}
