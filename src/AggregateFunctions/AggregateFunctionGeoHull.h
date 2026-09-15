#pragma once

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

    [[gnu::noinline]] static int exactSign(const std::array<double, 6> & coordinates)
    {
        std::array<uint64_t, 6> mantissas;
        std::array<unsigned, 6> exponents;
        unsigned common_exponent = 2046;
        for (size_t i = 0; i < coordinates.size(); ++i)
        {
            const auto bits = std::bit_cast<uint64_t>(coordinates[i]);
            const unsigned exponent = (bits >> 52) & 0x7ff;
            mantissas[i] = (bits & ((uint64_t{1} << 52) - 1)) | (exponent ? uint64_t{1} << 52 : 0);
            exponents[i] = exponent ? exponent - 1 : 0;
            if (mantissas[i])
                common_exponent = std::min(common_exponent, exponents[i]);
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
template <typename MultiPoint, typename Ring>
void computeGeoConvexHull(const MultiPoint & points, Ring & result)
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
    const auto left = *left_it;
    const auto right = *right_it;

    MultiPoint lower;
    MultiPoint upper;
    lower.push_back(left);
    upper.push_back(left);
    for (const auto & point : points)
    {
        const int side = GeoHullSide::apply(left, right, point);
        if (side < 0)
            lower.push_back(point);
        else if (side > 0)
            upper.push_back(point);
    }
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

}
