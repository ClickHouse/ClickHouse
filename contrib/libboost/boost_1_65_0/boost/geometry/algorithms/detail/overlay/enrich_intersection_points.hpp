// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2007-2012 Barend Gehrels, Amsterdam, the Netherlands.

// This file was modified by Oracle on 2017.
// Modifications copyright (c) 2017 Oracle and/or its affiliates.

// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_ENRICH_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_ENRICH_HPP

#include <cstddef>
#include <algorithm>
#include <map>
#include <set>
#include <vector>

#ifdef BOOST_GEOMETRY_DEBUG_ENRICH
#  include <iostream>
#  include <boost/geometry/algorithms/detail/overlay/debug_turn_info.hpp>
#  include <boost/geometry/io/wkt/wkt.hpp>
#  if ! defined(BOOST_GEOMETRY_DEBUG_IDENTIFIER)
#    define BOOST_GEOMETRY_DEBUG_IDENTIFIER
  #endif
#endif

#include <boost/range.hpp>

#include <boost/geometry/algorithms/detail/ring_identifier.hpp>
#include <boost/geometry/algorithms/detail/overlay/handle_colocations.hpp>
#include <boost/geometry/algorithms/detail/overlay/handle_self_turns.hpp>
#include <boost/geometry/algorithms/detail/overlay/is_self_turn.hpp>
#include <boost/geometry/algorithms/detail/overlay/less_by_segment_ratio.hpp>
#include <boost/geometry/algorithms/detail/overlay/overlay_type.hpp>
#include <boost/geometry/policies/robustness/robust_type.hpp>

#ifdef BOOST_GEOMETRY_DEBUG_ENRICH
#  include <boost/geometry/algorithms/detail/overlay/check_enrich.hpp>
#endif


namespace boost { namespace geometry
{

#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace overlay
{


// Sorts IP-s of this ring on segment-identifier, and if on same segment,
//  on distance.
// Then assigns for each IP which is the next IP on this segment,
// plus the vertex-index to travel to, plus the next IP
// (might be on another segment)
template
<
    bool Reverse1, bool Reverse2,
    typename Operations,
    typename Turns,
    typename Geometry1, typename Geometry2,
    typename RobustPolicy,
    typename SideStrategy
>
inline void enrich_sort(Operations& operations,
            Turns const& turns,
            operation_type for_operation,
            Geometry1 const& geometry1,
            Geometry2 const& geometry2,
            RobustPolicy const& robust_policy,
            SideStrategy const& strategy)
{
    std::sort(boost::begin(operations),
            boost::end(operations),
            less_by_segment_ratio
                <
                    Turns,
                    typename boost::range_value<Operations>::type,
                    Geometry1, Geometry2,
                    RobustPolicy,
                    SideStrategy,
                    Reverse1, Reverse2
                >(turns, for_operation, geometry1, geometry2, robust_policy, strategy));
}


template <typename Operations, typename Turns>
inline void enrich_assign(Operations& operations, Turns& turns)
{
    typedef typename boost::range_value<Turns>::type turn_type;
    typedef typename turn_type::turn_operation_type op_type;
    typedef typename boost::range_iterator<Operations>::type iterator_type;


    if (operations.size() > 0)
    {
        // Assign travel-to-vertex/ip index for each turning point.
        // Iterator "next" is circular

        geometry::ever_circling_range_iterator<Operations const> next(operations);
        ++next;

        for (iterator_type it = boost::begin(operations);
             it != boost::end(operations); ++it)
        {
            turn_type& turn = turns[it->turn_index];
            op_type& op = turn.operations[it->operation_index];

            // Normal behaviour: next should point at next turn:
            if (it->turn_index == next->turn_index)
            {
                ++next;
            }

            // Cluster behaviour: next should point after cluster, unless
            // their seg_ids are not the same
            while (turn.cluster_id != -1
                   && it->turn_index != next->turn_index
                   && turn.cluster_id == turns[next->turn_index].cluster_id
                   && op.seg_id == turns[next->turn_index].operations[next->operation_index].seg_id)
            {
                ++next;
            }

            turn_type const& next_turn = turns[next->turn_index];
            op_type const& next_op = next_turn.operations[next->operation_index];

            op.enriched.travels_to_ip_index
                    = static_cast<signed_size_type>(next->turn_index);
            op.enriched.travels_to_vertex_index
                    = next->subject->seg_id.segment_index;

            if (op.seg_id.segment_index == next_op.seg_id.segment_index
                    && op.fraction < next_op.fraction)
            {
                // Next turn is located further on same segment
                // assign next_ip_index
                // (this is one not circular therefore fraction is considered)
                op.enriched.next_ip_index = static_cast<signed_size_type>(next->turn_index);
            }
        }
    }

    // DEBUG
#ifdef BOOST_GEOMETRY_DEBUG_ENRICH
    {
        for (iterator_type it = boost::begin(operations);
             it != boost::end(operations);
             ++it)
        {
            op_type const& op = turns[it->turn_index]
                .operations[it->operation_index];

            std::cout << it->turn_index
                << " cl=" << turns[it->turn_index].cluster_id
                << " meth=" << method_char(turns[it->turn_index].method)
                << " seg=" << op.seg_id
                << " dst=" << op.fraction // needs define
                << " op=" << operation_char(turns[it->turn_index].operations[0].operation)
                << operation_char(turns[it->turn_index].operations[1].operation)
                << " (" << operation_char(op.operation) << ")"
                << " nxt=" << op.enriched.next_ip_index
                << " / " << op.enriched.travels_to_ip_index
                << " [vx " << op.enriched.travels_to_vertex_index << "]"
                << std::boolalpha << turns[it->turn_index].discarded
                << std::endl;
                ;
        }
    }
#endif
    // END DEBUG

}


template <typename Turns, typename MappedVector>
inline void create_map(Turns const& turns, MappedVector& mapped_vector)
{
    typedef typename boost::range_value<Turns>::type turn_type;
    typedef typename turn_type::container_type container_type;
    typedef typename MappedVector::mapped_type mapped_type;
    typedef typename boost::range_value<mapped_type>::type indexed_type;

    std::size_t index = 0;
    for (typename boost::range_iterator<Turns const>::type
            it = boost::begin(turns);
         it != boost::end(turns);
         ++it, ++index)
    {
        // Add all (non discarded) operations on this ring
        // Blocked operations or uu on clusters (for intersection)
        // should be included, to block potential paths in clusters
        turn_type const& turn = *it;
        if (turn.discarded)
        {
            continue;
        }

        std::size_t op_index = 0;
        for (typename boost::range_iterator<container_type const>::type
                op_it = boost::begin(turn.operations);
            op_it != boost::end(turn.operations);
            ++op_it, ++op_index)
        {
            ring_identifier const ring_id
                (
                    op_it->seg_id.source_index,
                    op_it->seg_id.multi_index,
                    op_it->seg_id.ring_index
                );
            mapped_vector[ring_id].push_back
                (
                    indexed_type(index, op_index, *op_it,
                        it->operations[1 - op_index].seg_id)
                );
        }
    }
}

template <typename Point1, typename Point2>
inline typename geometry::coordinate_type<Point1>::type
        distance_measure(Point1 const& a, Point2 const& b)
{
    // TODO: use comparable distance for point-point instead - but that
    // causes currently cycling include problems
    typedef typename geometry::coordinate_type<Point1>::type ctype;
    ctype const dx = get<0>(a) - get<0>(b);
    ctype const dy = get<1>(a) - get<1>(b);
    return dx * dx + dy * dy;
}

template <typename Turns>
inline void calculate_remaining_distance(Turns& turns)
{
    typedef typename boost::range_value<Turns>::type turn_type;
    typedef typename turn_type::turn_operation_type op_type;

    for (typename boost::range_iterator<Turns>::type
            it = boost::begin(turns);
         it != boost::end(turns);
         ++it)
    {
        turn_type& turn = *it;
        if (! turn.both(detail::overlay::operation_continue))
        {
           continue;
        }

        op_type& op0 = turn.operations[0];
        op_type& op1 = turn.operations[1];

        if (op0.remaining_distance != 0
         || op1.remaining_distance != 0)
        {
            continue;
        }

        int const to_index0 = op0.enriched.get_next_turn_index();
        int const to_index1 = op1.enriched.get_next_turn_index();
        if (to_index1 >= 0
                && to_index1 >= 0
                && to_index0 != to_index1)
        {
            op0.remaining_distance = distance_measure(turn.point, turns[to_index0].point);
            op1.remaining_distance = distance_measure(turn.point, turns[to_index1].point);
        }
    }
}


}} // namespace detail::overlay
#endif //DOXYGEN_NO_DETAIL



/*!
\brief All intersection points are enriched with successor information
\ingroup overlay
\tparam Turns type of intersection container
            (e.g. vector of "intersection/turn point"'s)
\tparam Clusters type of cluster container
\tparam Geometry1 \tparam_geometry
\tparam Geometry2 \tparam_geometry
\tparam SideStrategy side strategy type
\param turns container containing intersection points
\param clusters container containing clusters
\param geometry1 \param_geometry
\param geometry2 \param_geometry
\param robust_policy policy to handle robustness issues
\param strategy strategy
 */
template
<
    bool Reverse1, bool Reverse2,
    overlay_type OverlayType,
    typename Turns,
    typename Clusters,
    typename Geometry1, typename Geometry2,
    typename RobustPolicy,
    typename SideStrategy
>
inline void enrich_intersection_points(Turns& turns,
    Clusters& clusters,
    Geometry1 const& geometry1, Geometry2 const& geometry2,
    RobustPolicy const& robust_policy,
    SideStrategy const& strategy)
{
    static const detail::overlay::operation_type target_operation
            = detail::overlay::operation_from_overlay<OverlayType>::value;
    static const detail::overlay::operation_type opposite_operation
            = target_operation == detail::overlay::operation_union
            ? detail::overlay::operation_intersection
            : detail::overlay::operation_union;

    typedef typename boost::range_value<Turns>::type turn_type;
    typedef typename turn_type::turn_operation_type op_type;
    typedef detail::overlay::indexed_turn_operation
        <
            op_type
        > indexed_turn_operation;

    typedef std::map
        <
            ring_identifier,
            std::vector<indexed_turn_operation>
        > mapped_vector_type;

    bool has_cc = false;
    bool const has_colocations
        = detail::overlay::handle_colocations<Reverse1, Reverse2, OverlayType>(turns,
        clusters, geometry1, geometry2);

    // Discard turns not part of target overlay
    for (typename boost::range_iterator<Turns>::type
            it = boost::begin(turns);
         it != boost::end(turns);
         ++it)
    {
        turn_type& turn = *it;

        if (turn.both(detail::overlay::operation_none))
        {
            turn.discarded = true;
            continue;
        }

        if (turn.both(opposite_operation))
        {
            // For intersections, remove uu to avoid the need to travel
            // a union (during intersection) in uu/cc clusters (e.g. #31,#32,#33)
            // Also, for union, discard ii
            turn.discarded = true;
            turn.cluster_id = -1;
            continue;
        }

        if (detail::overlay::is_self_turn<OverlayType>(turn)
            && turn.cluster_id < 0
            && ! turn.both(target_operation))
        {
            // Only keep self-uu-turns or self-ii-turns
           turn.discarded = true;
           turn.cluster_id = -1;
           continue;
        }

        if (! turn.discarded
            && turn.both(detail::overlay::operation_continue))
        {
            has_cc = true;
        }
    }

    detail::overlay::discard_closed_turns
        <
            OverlayType,
            target_operation
        >::apply(turns, geometry1, geometry2);
    detail::overlay::discard_open_turns
        <
            OverlayType,
            target_operation
        >::apply(turns, geometry1, geometry2);

    // Create a map of vectors of indexed operation-types to be able
    // to sort intersection points PER RING
    mapped_vector_type mapped_vector;

    detail::overlay::create_map(turns, mapped_vector);

    // No const-iterator; contents of mapped copy is temporary,
    // and changed by enrich
    for (typename mapped_vector_type::iterator mit
        = mapped_vector.begin();
        mit != mapped_vector.end();
        ++mit)
    {
#ifdef BOOST_GEOMETRY_DEBUG_ENRICH
    std::cout << "ENRICH-sort Ring "
        << mit->first << std::endl;
#endif
        detail::overlay::enrich_sort<Reverse1, Reverse2>(
                    mit->second, turns, target_operation,
                    geometry1, geometry2,
                    robust_policy, strategy);
    }

    for (typename mapped_vector_type::iterator mit
        = mapped_vector.begin();
        mit != mapped_vector.end();
        ++mit)
    {
#ifdef BOOST_GEOMETRY_DEBUG_ENRICH
    std::cout << "ENRICH-assign Ring "
        << mit->first << std::endl;
#endif
        detail::overlay::enrich_assign(mit->second, turns);
    }

    if (has_colocations)
    {
        // First gather cluster properties (using even clusters with
        // discarded turns - for open turns), then clean up clusters
        detail::overlay::gather_cluster_properties
            <
                Reverse1,
                Reverse2,
                OverlayType
            >(clusters, turns, target_operation,
              geometry1, geometry2, strategy);

        detail::overlay::cleanup_clusters(turns, clusters);
    }

    if (has_cc)
    {
        detail::overlay::calculate_remaining_distance(turns);
    }

#ifdef BOOST_GEOMETRY_DEBUG_ENRICH
    //detail::overlay::check_graph(turns, for_operation);
#endif

}

}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_ENRICH_HPP
