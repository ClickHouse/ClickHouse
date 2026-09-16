#pragma once

#include <AggregateFunctions/AggregateFunctionGeoRectangle.h>


namespace DB
{

/// Two open orthogonal chains, both ordered from left to right. The caller validates the input
/// ring's topology; this representation additionally certifies monotonicity and local appendability.
/// Updating a suffix copies only the incoming chain. Materializing the closed ring is explicit.
struct GeoMonotoneRing
{
    CartesianRing lower;
    CartesianRing upper;

    size_t points() const
    {
        return lower.size() + upper.size() + 1;
    }

    static std::optional<GeoMonotoneRing> fromRing(const CartesianRing & ring)
    {
        if (ring.size() < 5 || ring.front().get<0>() != ring.back().get<0>() || ring.front().get<1>() != ring.back().get<1>())
            return std::nullopt;

        auto collinear = [](const CartesianPoint & a, const CartesianPoint & b, const CartesianPoint & c)
        {
            return (a.get<0>() == b.get<0>() && b.get<0>() == c.get<0>())
                || (a.get<1>() == b.get<1>() && b.get<1>() == c.get<1>());
        };
        CartesianRing vertices;
        for (size_t i = 0; i + 1 < ring.size(); ++i)
        {
            const auto & point = ring[i];
            const auto & next = ring[i + 1];
            if (!std::isfinite(point.get<0>()) || !std::isfinite(point.get<1>())
                || ((point.get<0>() == next.get<0>()) == (point.get<1>() == next.get<1>())))
                return std::nullopt;
            while (vertices.size() >= 2 && collinear(vertices[vertices.size() - 2], vertices.back(), point))
                vertices.pop_back();
            vertices.push_back(point);
        }
        while (vertices.size() >= 4 && collinear(vertices[vertices.size() - 2], vertices.back(), vertices.front()))
            vertices.pop_back();
        while (vertices.size() >= 4 && collinear(vertices.back(), vertices.front(), vertices[1]))
            vertices.erase(vertices.begin());
        if (vertices.size() < 4)
            return std::nullopt;

        size_t start = 0;
        Float64 right_x = vertices[0].get<0>();
        for (size_t i = 1; i < vertices.size(); ++i)
        {
            if (vertices[i].get<0>() < vertices[start].get<0>()
                || (vertices[i].get<0>() == vertices[start].get<0>() && vertices[i].get<1>() < vertices[start].get<1>()))
                start = i;
            right_x = std::max(right_x, vertices[i].get<0>());
        }
        const Float64 left_x = vertices[start].get<0>();
        if (!(left_x < right_x))
            return std::nullopt;
        const bool forward = vertices[(start + 1) % vertices.size()].get<0>() > left_x;
        auto advance = [&](size_t i)
        {
            return (i + (forward ? 1 : vertices.size() - 1)) % vertices.size();
        };

        GeoMonotoneRing result;
        size_t index = start;
        do
        {
            if (!result.lower.empty() && vertices[index].get<0>() < result.lower.back().get<0>())
                return std::nullopt;
            result.lower.push_back(vertices[index]);
            if (vertices[index].get<0>() == right_x)
                break;
            index = advance(index);
        } while (index != start);
        if (result.lower.size() < 2 || result.lower.back().get<0>() != right_x)
            return std::nullopt;

        index = advance(index);
        if (vertices[index].get<0>() != right_x || vertices[index].get<1>() <= result.lower.back().get<1>())
            return std::nullopt;
        do
        {
            if (!result.upper.empty() && vertices[index].get<0>() > result.upper.back().get<0>())
                return std::nullopt;
            result.upper.push_back(vertices[index]);
            if (vertices[index].get<0>() == left_x)
                break;
            index = advance(index);
        } while (index != start);
        if (result.upper.size() < 2 || result.upper.back().get<0>() != left_x
            || result.upper.back().get<1>() <= result.lower.front().get<1>() || advance(index) != start
            || result.lower.size() + result.upper.size() != vertices.size())
            return std::nullopt;
        std::reverse(result.upper.begin(), result.upper.end());
        return result;
    }

    bool canAppend(const GeoMonotoneRing & other) const
    {
        const auto left_x = other.lower.front().get<0>();
        const auto right_x = lower.back().get<0>();
        /// The overlap is confined to the constant-width end slabs of the two valid rings.
        /// A positive vertical overlap keeps their union one simple, monotone ring.
        return left_x < right_x && other.lower.back().get<0>() > right_x
            && left_x >= std::max(lower[lower.size() - 2].get<0>(), upper[upper.size() - 2].get<0>())
            && right_x <= std::min(other.lower[1].get<0>(), other.upper[1].get<0>())
            && std::max(lower.back().get<1>(), other.lower.front().get<1>())
                < std::min(upper.back().get<1>(), other.upper.front().get<1>());
    }

    void append(const GeoMonotoneRing & other)
    {
        auto extend = [](CartesianRing & chain, const CartesianRing & incoming, bool lower_chain)
        {
            auto endpoint = chain.back();
            const auto old_y = endpoint.get<1>();
            const auto new_y = incoming.front().get<1>();
            chain.pop_back();
            auto append_point = [&](const CartesianPoint & point)
            {
                /// When a slab ends exactly at the overlap boundary, the replaced vertical
                /// segments may cancel. Remove only collinear or duplicate junction vertices.
                while (chain.size() >= 2)
                {
                    const auto & a = chain[chain.size() - 2];
                    const auto & b = chain.back();
                    if (!((a.get<0>() == b.get<0>() && b.get<0>() == point.get<0>())
                            || (a.get<1>() == b.get<1>() && b.get<1>() == point.get<1>())))
                        break;
                    chain.pop_back();
                }
                if (chain.empty() || chain.back().get<0>() != point.get<0>() || chain.back().get<1>() != point.get<1>())
                    chain.push_back(point);
            };
            if (new_y != old_y)
            {
                if ((lower_chain && new_y < old_y) || (!lower_chain && new_y > old_y))
                    endpoint.set<0>(incoming.front().get<0>());
                append_point(endpoint);
                append_point({endpoint.get<0>(), new_y});
            }
            for (auto it = incoming.begin() + 1; it != incoming.end(); ++it)
                append_point(*it);
        };
        extend(lower, other.lower, true);
        extend(upper, other.upper, false);
    }

    CartesianRing materialize() const
    {
        CartesianRing result;
        result.reserve(points());
        result.insert(result.end(), lower.begin(), lower.end());
        result.insert(result.end(), upper.rbegin(), upper.rend());
        result.push_back(result.front());
        return result;
    }
};

}
