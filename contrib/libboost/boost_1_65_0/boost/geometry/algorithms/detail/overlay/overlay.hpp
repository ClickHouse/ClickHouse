// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2007-2015 Barend Gehrels, Amsterdam, the Netherlands.
// Copyright (c) 2013-2015 Adam Wulkiewicz, Lodz, Poland

// This file was modified by Oracle on 2015, 2017.
// Modifications copyright (c) 2015-2017, Oracle and/or its affiliates.

// Contributed and/or modified by Menelaos Karavelas, on behalf of Oracle
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_OVERLAY_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_OVERLAY_HPP


#include <deque>
#include <map>

#include <boost/range.hpp>
#include <boost/mpl/assert.hpp>


#include <boost/geometry/algorithms/detail/overlay/cluster_info.hpp>
#include <boost/geometry/algorithms/detail/overlay/enrich_intersection_points.hpp>
#include <boost/geometry/algorithms/detail/overlay/enrichment_info.hpp>
#include <boost/geometry/algorithms/detail/overlay/get_turns.hpp>
#include <boost/geometry/algorithms/detail/overlay/is_self_turn.hpp>
#include <boost/geometry/algorithms/detail/overlay/overlay_type.hpp>
#include <boost/geometry/algorithms/detail/overlay/traverse.hpp>
#include <boost/geometry/algorithms/detail/overlay/traversal_info.hpp>
#include <boost/geometry/algorithms/detail/overlay/self_turn_points.hpp>
#include <boost/geometry/algorithms/detail/overlay/turn_info.hpp>

#include <boost/geometry/algorithms/detail/recalculate.hpp>

#include <boost/geometry/algorithms/is_empty.hpp>
#include <boost/geometry/algorithms/reverse.hpp>

#include <boost/geometry/algorithms/detail/overlay/add_rings.hpp>
#include <boost/geometry/algorithms/detail/overlay/assign_parents.hpp>
#include <boost/geometry/algorithms/detail/overlay/ring_properties.hpp>
#include <boost/geometry/algorithms/detail/overlay/select_rings.hpp>
#include <boost/geometry/algorithms/detail/overlay/do_reverse.hpp>

#include <boost/geometry/policies/robustness/segment_ratio_type.hpp>


#ifdef BOOST_GEOMETRY_DEBUG_ASSEMBLE
#  include <boost/geometry/io/dsv/write.hpp>
#endif


namespace boost { namespace geometry
{


#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace overlay
{


//! Default visitor for overlay, doing nothing
struct overlay_null_visitor
{
    void print(char const* ) {}

    template <typename Turns>
    void print(char const* , Turns const& , int) {}

    template <typename Turns>
    void print(char const* , Turns const& , int , int ) {}

    template <typename Turns>
    void visit_turns(int , Turns const& ) {}

    template <typename Clusters, typename Turns>
    void visit_clusters(Clusters const& , Turns const& ) {}

    template <typename Turns, typename Turn, typename Operation>
    void visit_traverse(Turns const& , Turn const& , Operation const& , char const*)
    {}

    template <typename Turns, typename Turn, typename Operation>
    void visit_traverse_reject(Turns const& , Turn const& , Operation const& , traverse_error_type )
    {}
};

template
<
    overlay_type OverlayType,
    typename TurnInfoMap,
    typename Turns,
    typename Clusters
>
inline void get_ring_turn_info(TurnInfoMap& turn_info_map, Turns const& turns, Clusters const& clusters)
{
    typedef typename boost::range_value<Turns>::type turn_type;
    typedef typename turn_type::container_type container_type;

    static const operation_type target_operation
            = operation_from_overlay<OverlayType>::value;
    static const operation_type opposite_operation
            = target_operation == operation_union ? operation_intersection : operation_union;

    signed_size_type turn_index = 0;
    for (typename boost::range_iterator<Turns const>::type
            it = boost::begin(turns);
         it != boost::end(turns);
         ++it, turn_index++)
    {
        typename boost::range_value<Turns>::type const& turn = *it;

        bool const colocated_target = target_operation == operation_union
                ? turn.colocated_uu : turn.colocated_ii;
        bool const colocated_opp = target_operation == operation_union
                ? turn.colocated_ii : turn.colocated_uu;
        bool const both_opposite = turn.both(opposite_operation);

        bool const traversed
                = turn.operations[0].visited.finalized()
                || turn.operations[0].visited.rejected()
                || turn.operations[1].visited.finalized()
                || turn.operations[1].visited.rejected()
                || turn.both(operation_blocked)
                || turn.combination(opposite_operation, operation_blocked);

        bool is_closed = false;
        if (turn.cluster_id >= 0 && target_operation == operation_union)
        {
            typename Clusters::const_iterator mit = clusters.find(turn.cluster_id);
            BOOST_ASSERT(mit != clusters.end());

            cluster_info const& cinfo = mit->second;
            is_closed = cinfo.open_count == 0;
        }

        for (typename boost::range_iterator<container_type const>::type
                op_it = boost::begin(turn.operations);
            op_it != boost::end(turn.operations);
            ++op_it)
        {
            ring_identifier const ring_id
                (
                    op_it->seg_id.source_index,
                    op_it->seg_id.multi_index,
                    op_it->seg_id.ring_index
                );

            if (traversed || is_closed || ! op_it->enriched.startable)
            {
                turn_info_map[ring_id].has_traversed_turn = true;
            }
            else if (both_opposite && colocated_target)
            {
                // For union: ii, colocated with a uu
                // For example, two interior rings touch where two exterior rings also touch.
                // The interior rings are not yet traversed, and should be taken from the input

                // For intersection: uu, colocated with an ii
                // unless it is two interior inner rings colocated with a uu

                // So don't set has_traversed_turn here
            }
            else if (both_opposite && ! is_self_turn<OverlayType>(turn))
            {
                // For union, mark any ring with a ii turn as traversed
                // For intersection, any uu - but not if it is a self-turn
                turn_info_map[ring_id].has_traversed_turn = true;
            }
            else if (colocated_opp && ! colocated_target)
            {
                // For union, a turn colocated with ii and NOT with uu/ux
                // For intersection v.v.
                turn_info_map[ring_id].has_traversed_turn = true;
            }
        }
    }
}


template
<
    typename GeometryOut, overlay_type OverlayType, bool ReverseOut,
    typename Geometry1, typename Geometry2,
    typename OutputIterator, typename Strategy
>
inline OutputIterator return_if_one_input_is_empty(Geometry1 const& geometry1,
            Geometry2 const& geometry2,
            OutputIterator out, Strategy const& strategy)
{
    typedef std::deque
        <
            typename geometry::ring_type<GeometryOut>::type
        > ring_container_type;

    typedef typename geometry::point_type<Geometry1>::type point_type1;

    typedef ring_properties
        <
            point_type1,
            typename Strategy::template area_strategy
                <
                    point_type1
                >::type::return_type
        > properties;

// Silence warning C4127: conditional expression is constant
#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4127)
#endif

    // Union: return either of them
    // Intersection: return nothing
    // Difference: return first of them
    if (OverlayType == overlay_intersection
        || (OverlayType == overlay_difference && geometry::is_empty(geometry1)))
    {
        return out;
    }

#if defined(_MSC_VER)
#pragma warning(pop)
#endif


    std::map<ring_identifier, ring_turn_info> empty;
    std::map<ring_identifier, properties> all_of_one_of_them;

    select_rings<OverlayType>(geometry1, geometry2, empty, all_of_one_of_them, strategy);
    ring_container_type rings;
    assign_parents(geometry1, geometry2, rings, all_of_one_of_them, strategy);
    return add_rings<GeometryOut>(all_of_one_of_them, geometry1, geometry2, rings, out);
}


template
<
    typename Geometry1, typename Geometry2,
    bool Reverse1, bool Reverse2, bool ReverseOut,
    typename GeometryOut,
    overlay_type OverlayType
>
struct overlay
{
    template <typename RobustPolicy, typename OutputIterator, typename Strategy, typename Visitor>
    static inline OutputIterator apply(
                Geometry1 const& geometry1, Geometry2 const& geometry2,
                RobustPolicy const& robust_policy,
                OutputIterator out,
                Strategy const& strategy,
                Visitor& visitor)
    {
        bool const is_empty1 = geometry::is_empty(geometry1);
        bool const is_empty2 = geometry::is_empty(geometry2);

        if (is_empty1 && is_empty2)
        {
            return out;
        }

        if (is_empty1 || is_empty2)
        {
            return return_if_one_input_is_empty
                <
                    GeometryOut, OverlayType, ReverseOut
                >(geometry1, geometry2, out, strategy);
        }

        typedef typename geometry::point_type<GeometryOut>::type point_type;
        typedef detail::overlay::traversal_turn_info
        <
            point_type,
            typename geometry::segment_ratio_type<point_type, RobustPolicy>::type
        > turn_info;
        typedef std::deque<turn_info> turn_container_type;

        typedef std::deque
            <
                typename geometry::ring_type<GeometryOut>::type
            > ring_container_type;

        // Define the clusters, mapping cluster_id -> turns
        typedef std::map
            <
                signed_size_type,
                cluster_info
            > cluster_type;

        turn_container_type turns;

#ifdef BOOST_GEOMETRY_DEBUG_ASSEMBLE
std::cout << "get turns" << std::endl;
#endif
        detail::get_turns::no_interrupt_policy policy;
        geometry::get_turns
            <
                Reverse1, Reverse2,
                detail::overlay::assign_null_policy
            >(geometry1, geometry2, strategy, robust_policy, turns, policy);

        visitor.visit_turns(1, turns);

#ifdef BOOST_GEOMETRY_INCLUDE_SELF_TURNS
        {
            self_get_turn_points::self_turns<Reverse1, assign_null_policy>(geometry1,
                strategy, robust_policy, turns, policy, 0);
            self_get_turn_points::self_turns<Reverse2, assign_null_policy>(geometry2,
                strategy, robust_policy, turns, policy, 1);
        }
#endif


#ifdef BOOST_GEOMETRY_DEBUG_ASSEMBLE
std::cout << "enrich" << std::endl;
#endif
        typename Strategy::side_strategy_type side_strategy = strategy.get_side_strategy();
        cluster_type clusters;

        geometry::enrich_intersection_points<Reverse1, Reverse2, OverlayType>(turns,
                clusters, geometry1, geometry2,
                    robust_policy,
                    side_strategy);

        visitor.visit_turns(2, turns);

        visitor.visit_clusters(clusters, turns);

#ifdef BOOST_GEOMETRY_DEBUG_ASSEMBLE
std::cout << "traverse" << std::endl;
#endif
        // Traverse through intersection/turn points and create rings of them.
        // Note that these rings are always in clockwise order, even in CCW polygons,
        // and are marked as "to be reversed" below
        ring_container_type rings;
        traverse<Reverse1, Reverse2, Geometry1, Geometry2, OverlayType>::apply
                (
                    geometry1, geometry2,
                    strategy,
                    robust_policy,
                    turns, rings,
                    clusters,
                    visitor
                );

        std::map<ring_identifier, ring_turn_info> turn_info_per_ring;
        get_ring_turn_info<OverlayType>(turn_info_per_ring, turns, clusters);

        typedef typename Strategy::template area_strategy<point_type>::type area_strategy_type;

        typedef ring_properties
            <
                point_type,
                typename area_strategy_type::return_type
            > properties;

        // Select all rings which are NOT touched by any intersection point
        std::map<ring_identifier, properties> selected_ring_properties;
        select_rings<OverlayType>(geometry1, geometry2, turn_info_per_ring,
                selected_ring_properties, strategy);

        // Add rings created during traversal
        {
            area_strategy_type const area_strategy = strategy.template get_area_strategy<point_type>();

            ring_identifier id(2, 0, -1);
            for (typename boost::range_iterator<ring_container_type>::type
                    it = boost::begin(rings);
                 it != boost::end(rings);
                 ++it)
            {
                selected_ring_properties[id] = properties(*it, area_strategy);
                selected_ring_properties[id].reversed = ReverseOut;
                id.multi_index++;
            }
        }

        assign_parents(geometry1, geometry2, rings, selected_ring_properties, strategy);

        return add_rings<GeometryOut>(selected_ring_properties, geometry1, geometry2, rings, out);
    }

    template <typename RobustPolicy, typename OutputIterator, typename Strategy>
    static inline OutputIterator apply(
                Geometry1 const& geometry1, Geometry2 const& geometry2,
                RobustPolicy const& robust_policy,
                OutputIterator out,
                Strategy const& strategy)
    {
        overlay_null_visitor visitor;
        return apply(geometry1, geometry2, robust_policy, out, strategy, visitor);
    }
};


}} // namespace detail::overlay
#endif // DOXYGEN_NO_DETAIL


}} // namespace boost::geometry


#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_OVERLAY_HPP
