#pragma once

#include <AggregateFunctions/AggregateFunctionGeoMonotoneRing.h>
#include <AggregateFunctions/AggregateFunctionGeoRectangle.h>
#include <Common/AllocatorWithMemoryTracking.h>

#include <boost/container/devector.hpp>
#include <map>


namespace DB
{

using GeoMonotoneDeque = boost::container::devector<CartesianPoint, AllocatorWithMemoryTracking<CartesianPoint>>; // STYLE_CHECK_ALLOW_STD_CONTAINERS
using GeoBidirectionalMonotoneRing = GeoMonotoneRingImpl<GeoMonotoneDeque>;

/// Valid monotone rings whose closed x projections are separated by more than `margin`
/// (see `geoSeparationMargin`). An insertion may join only the end slabs of at most two
/// neighbours. No unaffected ring is read or copied.
struct GeoMonotoneComponents
{
    using Value = std::pair<const Float64, GeoBidirectionalMonotoneRing>;
    using Rings = std::map<Float64, GeoBidirectionalMonotoneRing, std::less<>, AllocatorWithMemoryTracking<Value>>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    Rings rings;
    size_t total_points = 0;
    /// Must be derived from a bound on the absolute value of every coordinate of every ring.
    Float64 margin;

    explicit GeoMonotoneComponents(Float64 margin_) : margin(margin_) {}

    template <typename Chain>
    static GeoBidirectionalMonotoneRing copyRing(const GeoMonotoneRingImpl<Chain> & source)
    {
        GeoBidirectionalMonotoneRing result;
        result.lower.assign(source.lower.begin(), source.lower.end());
        result.upper.assign(source.upper.begin(), source.upper.end());
        return result;
    }

    template <typename Chain>
    bool insertDisjoint(const GeoMonotoneRingImpl<Chain> & incoming)
    {
        const auto left_x = incoming.lower.front().template get<0>();
        const auto right_x = incoming.lower.back().template get<0>();
        auto next = rings.lower_bound(left_x);
        if ((next != rings.end() && !geoSeparatedBy(right_x, next->first, margin))
            || (next != rings.begin() && !geoSeparatedBy(std::prev(next)->second.lower.back().template get<0>(), left_x, margin)))
            return false;
        rings.emplace_hint(next, left_x, copyRing(incoming));
        total_points += incoming.points();
        return true;
    }

    template <typename Chain>
    bool insert(const GeoMonotoneRingImpl<Chain> & incoming)
    {
        const auto left_x = incoming.lower.front().template get<0>();
        const auto right_x = incoming.lower.back().template get<0>();
        /// Rings closer than `margin` count as neighbours: they must either be joined through a
        /// local certificate, or the caller falls back to the general overlay.
        auto first = rings.lower_bound(left_x);
        if (first != rings.begin())
        {
            auto previous = std::prev(first);
            if (!geoSeparatedBy(previous->second.lower.back().template get<0>(), left_x, margin))
                first = previous;
        }
        auto end = first;
        size_t neighbours = 0;
        while (end != rings.end() && !geoSeparatedBy(right_x, end->first, margin))
        {
            if (++neighbours > 2)
                return false;
            ++end;
        }
        if (!neighbours)
        {
            rings.emplace_hint(first, left_x, copyRing(incoming));
            total_points += incoming.points();
            return true;
        }

        auto second = std::next(first);
        const bool append = first->second.canAppend(incoming);
        const bool prepend = incoming.canAppend(first->second);
        /// Check both bridge junctions before changing either old component. Touching,
        /// containment and internal overlaps do not satisfy these local certificates.
        if (neighbours == 1 ? !append && !prepend : !append || !incoming.canAppend(second->second))
            return false;

        size_t removed_points = first->second.points();
        if (neighbours == 2)
            removed_points += second->second.points();

        /// Reuse the larger old component, or copy a larger incoming component once.
        /// End insertion in `devector` avoids shifting the growing prefix on reverse input.
        GeoBidirectionalMonotoneRing joined;
        if (incoming.points() >= first->second.points()
            && (neighbours == 1 || incoming.points() >= second->second.points()))
        {
            joined = copyRing(incoming);
            if (append)
                joined.prepend(first->second);
            else
                joined.append(first->second);
            if (neighbours == 2)
                joined.append(second->second);
        }
        else if (neighbours == 2 && second->second.points() > first->second.points())
        {
            joined = std::move(second->second);
            joined.prepend(incoming);
            joined.prepend(first->second);
        }
        else
        {
            joined = std::move(first->second);
            if (append)
                joined.append(incoming);
            else
                joined.prepend(incoming);
            if (neighbours == 2)
                joined.append(second->second);
        }
        total_points = total_points - removed_points + joined.points();
        const auto joined_left = joined.lower.front().get<0>();
        rings.erase(first, end);
        rings.emplace_hint(end, joined_left, std::move(joined));
        return true;
    }
};

}
