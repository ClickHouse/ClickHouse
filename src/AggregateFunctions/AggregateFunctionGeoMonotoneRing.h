#pragma once

#include <AggregateFunctions/AggregateFunctionGeoRectangle.h>


namespace DB
{

/// Two open orthogonal chains, both ordered from left to right. The caller validates the input
/// ring's topology; this representation additionally certifies monotonicity and local appendability.
/// Updating a suffix copies only the incoming chain. Materializing the closed ring is explicit.
template <typename Chain>
struct GeoMonotoneRingImpl
{
    Chain lower;
    Chain upper;

    size_t points() const
    {
        return lower.size() + upper.size() + 1;
    }

    static std::optional<GeoMonotoneRingImpl> fromRing(const CartesianRing & ring)
    {
        if (ring.size() < 5 || ring.front().template get<0>() != ring.back().template get<0>() || ring.front().template get<1>() != ring.back().template get<1>())
            return std::nullopt;

        auto collinear = [](const CartesianPoint & a, const CartesianPoint & b, const CartesianPoint & c)
        {
            return (a.template get<0>() == b.template get<0>() && b.template get<0>() == c.template get<0>())
                || (a.template get<1>() == b.template get<1>() && b.template get<1>() == c.template get<1>());
        };
        CartesianRing vertices;
        for (size_t i = 0; i + 1 < ring.size(); ++i)
        {
            const auto & point = ring[i];
            const auto & next = ring[i + 1];
            if (!std::isfinite(point.template get<0>()) || !std::isfinite(point.template get<1>())
                || ((point.template get<0>() == next.template get<0>()) == (point.template get<1>() == next.template get<1>())))
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
        Float64 right_x = vertices[0].template get<0>();
        for (size_t i = 1; i < vertices.size(); ++i)
        {
            if (vertices[i].template get<0>() < vertices[start].template get<0>()
                || (vertices[i].template get<0>() == vertices[start].template get<0>() && vertices[i].template get<1>() < vertices[start].template get<1>()))
                start = i;
            right_x = std::max(right_x, vertices[i].template get<0>());
        }
        const Float64 left_x = vertices[start].template get<0>();
        if (!(left_x < right_x))
            return std::nullopt;
        const bool forward = vertices[(start + 1) % vertices.size()].template get<0>() > left_x;
        auto advance = [&](size_t i)
        {
            return (i + (forward ? 1 : vertices.size() - 1)) % vertices.size();
        };

        GeoMonotoneRingImpl result;
        size_t index = start;
        do
        {
            if (!result.lower.empty() && vertices[index].template get<0>() < result.lower.back().template get<0>())
                return std::nullopt;
            result.lower.push_back(vertices[index]);
            if (vertices[index].template get<0>() == right_x)
                break;
            index = advance(index);
        } while (index != start);
        if (result.lower.size() < 2 || result.lower.back().template get<0>() != right_x)
            return std::nullopt;

        index = advance(index);
        if (vertices[index].template get<0>() != right_x || vertices[index].template get<1>() <= result.lower.back().template get<1>())
            return std::nullopt;
        do
        {
            if (!result.upper.empty() && vertices[index].template get<0>() > result.upper.back().template get<0>())
                return std::nullopt;
            result.upper.push_back(vertices[index]);
            if (vertices[index].template get<0>() == left_x)
                break;
            index = advance(index);
        } while (index != start);
        if (result.upper.size() < 2 || result.upper.back().template get<0>() != left_x
            || result.upper.back().template get<1>() <= result.lower.front().template get<1>() || advance(index) != start
            || result.lower.size() + result.upper.size() != vertices.size())
            return std::nullopt;
        std::reverse(result.upper.begin(), result.upper.end());
        return result;
    }

    template <typename OtherChain>
    bool canAppend(const GeoMonotoneRingImpl<OtherChain> & other) const
    {
        const auto left_x = other.lower.front().template get<0>();
        const auto right_x = lower.back().template get<0>();
        /// The overlap is confined to the constant-width end slabs of the two valid rings.
        /// A positive vertical overlap keeps their union one simple, monotone ring.
        return left_x < right_x && other.lower.back().template get<0>() > right_x
            && left_x >= std::max(lower[lower.size() - 2].template get<0>(), upper[upper.size() - 2].template get<0>())
            && right_x <= std::min(other.lower[1].template get<0>(), other.upper[1].template get<0>())
            && std::max(lower.back().template get<1>(), other.lower.front().template get<1>())
                < std::min(upper.back().template get<1>(), other.upper.front().template get<1>());
    }

    template <typename OtherChain>
    void append(const GeoMonotoneRingImpl<OtherChain> & other)
    {
        extend<false>(lower, other.lower, true);
        extend<false>(upper, other.upper, false);
    }

    template <typename OtherChain>
    void prepend(const GeoMonotoneRingImpl<OtherChain> & other)
    {
        extend<true>(lower, other.lower, true);
        extend<true>(upper, other.upper, false);
    }

private:
    template <bool Prepend, typename OtherChain>
    static void extend(Chain & chain, const OtherChain & incoming, bool lower_chain)
    {
        auto last = [&](size_t offset) -> const CartesianPoint &
        {
            return chain[Prepend ? offset : chain.size() - 1 - offset];
        };
        auto remove_last = [&]
        {
            if constexpr (Prepend)
                chain.pop_front();
            else
                chain.pop_back();
        };
        const auto & incoming_end = Prepend ? incoming.back() : incoming.front();
        auto endpoint = last(0);
        const auto old_y = endpoint.template get<1>();
        const auto new_y = incoming_end.template get<1>();
        remove_last();
        auto append_point = [&](const CartesianPoint & point)
        {
            /// Remove only duplicate or collinear vertices at the modified junction.
            while (chain.size() >= 2)
            {
                const auto & a = last(1);
                const auto & b = last(0);
                if (!((a.template get<0>() == b.template get<0>() && b.template get<0>() == point.template get<0>())
                        || (a.template get<1>() == b.template get<1>() && b.template get<1>() == point.template get<1>())))
                    break;
                remove_last();
            }
            if (chain.empty() || last(0).template get<0>() != point.template get<0>() || last(0).template get<1>() != point.template get<1>())
            {
                if constexpr (Prepend)
                    chain.push_front(point);
                else
                    chain.push_back(point);
            }
        };
        if (new_y != old_y)
        {
            if ((lower_chain && new_y < old_y) || (!lower_chain && new_y > old_y))
                endpoint.template set<0>(incoming_end.template get<0>());
            append_point(endpoint);
            append_point({endpoint.template get<0>(), new_y});
        }
        for (size_t i = 1; i < incoming.size(); ++i)
            append_point(incoming[Prepend ? incoming.size() - 1 - i : i]);
    }

public:
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

using GeoMonotoneRing = GeoMonotoneRingImpl<CartesianRing>;

}
