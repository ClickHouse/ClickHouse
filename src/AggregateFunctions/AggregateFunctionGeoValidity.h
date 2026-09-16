#pragma once

// Adapted from Boost.Geometry's algorithms/detail/is_valid/multipolygon.hpp.
// Copyright (c) 2023 Adam Wulkiewicz, Lodz, Poland.
// Copyright (c) 2014-2021, Oracle and/or its affiliates.
// Licensed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#include <boost/geometry/algorithms/detail/is_valid/multipolygon.hpp>
#include <boost/geometry/policies/is_valid/default_policy.hpp>
#include <boost/geometry/policies/is_valid/failing_reason_policy.hpp>
#include <boost/geometry/strategies/relate/services.hpp>
#include <boost/iterator/indirect_iterator.hpp>

#include <deque>
#include <memory>
#include <sstream>
#include <vector>


namespace DB
{

/// Preserve all five Boost validation phases and their order. Index intra-polygon turns once
/// instead of constructing a filter over the entire turn list for every polygon in phases 3/4.
/// The index preserves turn order and the original turns remain available to phase 5.
template <typename MultiPolygon, template <typename> class Allocator = std::allocator>
class GeoMultiPolygonValidity
    : private boost::geometry::detail::is_valid::is_valid_polygon<typename boost::range_value<MultiPolygon>::type, true>
{
    using Base = boost::geometry::detail::is_valid::is_valid_polygon<typename boost::range_value<MultiPolygon>::type, true>;

    template <typename T>
    using Vector = std::vector<T, Allocator<T>>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    template <typename Turns, typename Visitor, typename Strategy>
    static bool disjointInteriors(const MultiPolygon & geometry, const Turns & turns, Visitor & visitor, const Strategy & strategy)
    {
        namespace bg = boost::geometry;
        Vector<unsigned char> crossing(geometry.size(), 0);
        for (const auto & turn : turns)
        {
            if (!turn.touch_only)
            {
                for (const auto & operation : turn.operations)
                {
                    const auto index = operation.seg_id.multi_index;
                    if (index >= 0 && static_cast<size_t>(index) < crossing.size())
                        crossing[index] = 1;
                }
            }
        }

        using Box = bg::model::box<bg::point_type_t<MultiPolygon>>;
        using Item = typename Base::template partition_item<decltype(geometry.begin()), Box>;
        Vector<Item> items;
        size_t index = 0;
        for (auto it = geometry.begin(); it != geometry.end(); ++it, ++index)
            if (!crossing[index])
                items.emplace_back(it);

        typename Base::template item_visitor_type<Strategy> item_visitor(strategy);
        bg::partition<Box>::apply(
            items,
            item_visitor,
            typename Base::template expand_box<Strategy>(strategy),
            typename Base::template overlaps_box<Strategy>(strategy));

        if (item_visitor.items_overlap)
            return visitor.template apply<bg::failure_intersecting_interiors>();
        return visitor.template apply<bg::no_failure>();
    }

public:
    template <typename Visitor, typename Strategy>
    static bool apply(const MultiPolygon & geometry, Visitor & visitor, const Strategy & strategy)
    {
        namespace bg = boost::geometry;
        if (geometry.empty())
            return visitor.template apply<bg::no_failure>();

        /// Phase 1: ring validity. Cross-ring relationships are checked below.
        for (const auto & polygon : geometry)
            if (!Base::apply(polygon, visitor, strategy))
                return false;

        /// Phase 2: collect self-turns and reject unacceptable intersections.
        using SelfTurns = bg::detail::is_valid::has_valid_self_turns<MultiPolygon, typename Strategy::cs_tag>;
        using Turn = typename SelfTurns::turn_type;
        std::deque<Turn, Allocator<Turn>> turns; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        if (!SelfTurns::apply(geometry, turns, visitor, strategy))
            return false;

        /// Compressed row index: O(P + T) work/storage, with no copies of turn records.
        Vector<size_t> offsets(geometry.size() + 1, 0);
        auto intra_polygon = [&](const Turn & turn)
        {
            const auto index = turn.operations[0].seg_id.multi_index;
            return index >= 0 && static_cast<size_t>(index) < geometry.size()
                && index == turn.operations[1].seg_id.multi_index;
        };
        for (const auto & turn : turns)
            if (intra_polygon(turn))
                ++offsets[turn.operations[0].seg_id.multi_index + 1];
        for (size_t i = 1; i < offsets.size(); ++i)
            offsets[i] += offsets[i - 1];

        Vector<const Turn *> indexed_turns(offsets.back());
        {
            auto positions = offsets;
            for (const auto & turn : turns)
                if (intra_polygon(turn))
                    indexed_turns[positions[turn.operations[0].seg_id.multi_index]++] = &turn;
        }

        /// Phase 3: holes inside their exterior, with no nested holes.
        for (size_t i = 0; i < geometry.size(); ++i)
        {
            auto first = boost::make_indirect_iterator(indexed_turns.begin() + offsets[i]);
            auto beyond = boost::make_indirect_iterator(indexed_turns.begin() + offsets[i + 1]);
            if (!Base::has_holes_inside::apply(geometry[i], first, beyond, visitor, strategy))
                return false;
        }

        /// Phase 4: connected polygon interiors, including rings joined by touch points.
        for (size_t i = 0; i < geometry.size(); ++i)
        {
            auto first = boost::make_indirect_iterator(indexed_turns.begin() + offsets[i]);
            auto beyond = boost::make_indirect_iterator(indexed_turns.begin() + offsets[i + 1]);
            if (!Base::has_connected_interior::apply(geometry[i], first, beyond, visitor, strategy))
                return false;
        }

        /// Phase 5: pairwise disjoint interiors. Inter-polygon turns must remain available.
        return disjointInteriors(geometry, turns, visitor, strategy);
    }
};

/// Preserve Boost's accepted failures and format the same diagnostic only on rejection.
class GeoValidityFailureVisitor
{
    std::string & reason;

public:
    explicit GeoValidityFailureVisitor(std::string & reason_) : reason(reason_)
    {
    }

    template <boost::geometry::validity_failure_type Failure, typename... Data>
    bool apply(const Data &... data)
    {
        if (boost::geometry::is_valid_default_policy<>::template apply<Failure>(data...))
            return true;

        /// Boost's failure visitor requires a string stream.
        std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        boost::geometry::failing_reason_policy<> visitor(stream);
        visitor.template apply<Failure>(data...);
        reason = stream.str();
        return false;
    }
};

template <template <typename> class Allocator = std::allocator, typename MultiPolygon>
bool isValidGeoMultiPolygon(const MultiPolygon & geometry, std::string & reason)
{
    using Strategy = typename boost::geometry::strategies::relate::services::default_strategy<MultiPolygon, MultiPolygon>::type;
    GeoValidityFailureVisitor visitor(reason);
    const bool valid = GeoMultiPolygonValidity<MultiPolygon, Allocator>::apply(geometry, visitor, Strategy{});
    if (valid)
        reason = boost::geometry::validity_failure_type_message(boost::geometry::no_failure);
    return valid;
}

}
