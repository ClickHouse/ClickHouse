// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2007-2014 Barend Gehrels, Amsterdam, the Netherlands.

// This file was modified by Oracle on 2014, 2017.
// Modifications copyright (c) 2014-2017 Oracle and/or its affiliates.
// Contributed and/or modified by Menelaos Karavelas, on behalf of Oracle
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_FOLLOW_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_FOLLOW_HPP

#include <cstddef>

#include <boost/range.hpp>
#include <boost/mpl/assert.hpp>

#include <boost/geometry/algorithms/detail/point_on_border.hpp>
#include <boost/geometry/algorithms/detail/overlay/append_no_duplicates.hpp>
#include <boost/geometry/algorithms/detail/overlay/copy_segments.hpp>
#include <boost/geometry/algorithms/detail/overlay/turn_info.hpp>

#include <boost/geometry/algorithms/covered_by.hpp>
#include <boost/geometry/algorithms/clear.hpp>


namespace boost { namespace geometry
{


#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace overlay
{

namespace following
{

template <typename Turn, typename Operation>
static inline bool is_entering(Turn const& /* TODO remove this parameter */, Operation const& op)
{
    // (Blocked means: blocked for polygon/polygon intersection, because
    // they are reversed. But for polygon/line it is similar to continue)
    return op.operation == operation_intersection
        || op.operation == operation_continue
        || op.operation == operation_blocked
        ;
}

template
<
    typename Turn,
    typename Operation,
    typename LineString,
    typename Polygon,
    typename PtInPolyStrategy
>
static inline bool last_covered_by(Turn const& turn, Operation const& op,
                LineString const& linestring, Polygon const& polygon,
                PtInPolyStrategy const& strategy)
{
    return geometry::covered_by(range::at(linestring, op.seg_id.segment_index), polygon, strategy);
}


template
<
    typename Turn,
    typename Operation,
    typename LineString,
    typename Polygon,
    typename PtInPolyStrategy
>
static inline bool is_leaving(Turn const& turn, Operation const& op,
                bool entered, bool first,
                LineString const& linestring, Polygon const& polygon,
                PtInPolyStrategy const& strategy)
{
    if (op.operation == operation_union)
    {
        return entered
            || turn.method == method_crosses
            || (first && last_covered_by(turn, op, linestring, polygon, strategy))
            ;
    }
    return false;
}


template
<
    typename Turn,
    typename Operation,
    typename LineString,
    typename Polygon,
    typename PtInPolyStrategy
>
static inline bool is_staying_inside(Turn const& turn, Operation const& op,
                bool entered, bool first,
                LineString const& linestring, Polygon const& polygon,
                PtInPolyStrategy const& strategy)
{
    if (turn.method == method_crosses)
    {
        // The normal case, this is completely covered with entering/leaving
        // so stay out of this time consuming "covered_by"
        return false;
    }

    if (is_entering(turn, op))
    {
        return entered || (first && last_covered_by(turn, op, linestring, polygon, strategy));
    }

    return false;
}

template
<
    typename Turn,
    typename Operation,
    typename Linestring,
    typename Polygon,
    typename PtInPolyStrategy
>
static inline bool was_entered(Turn const& turn, Operation const& op, bool first,
                Linestring const& linestring, Polygon const& polygon,
                PtInPolyStrategy const& strategy)
{
    if (first && (turn.method == method_collinear || turn.method == method_equal))
    {
        return last_covered_by(turn, op, linestring, polygon, strategy);
    }
    return false;
}


// Template specialization structure to call the right actions for the right type
template <overlay_type OverlayType, bool RemoveSpikes = true>
struct action_selector
{
    // If you get here the overlay type is not intersection or difference
    // BOOST_MPL_ASSERT(false);
};

// Specialization for intersection, containing the implementation
template <bool RemoveSpikes>
struct action_selector<overlay_intersection, RemoveSpikes>
{
    template
    <
        typename OutputIterator,
        typename LineStringOut,
        typename LineString,
        typename Point,
        typename Operation,
        typename SideStrategy,
        typename RobustPolicy
    >
    static inline void enter(LineStringOut& current_piece,
                LineString const& ,
                segment_identifier& segment_id,
                signed_size_type , Point const& point,
                Operation const& operation,
                SideStrategy const& ,
                RobustPolicy const& ,
                OutputIterator& )
    {
        // On enter, append the intersection point and remember starting point
        // TODO: we don't check on spikes for linestrings (?). Consider this.
        detail::overlay::append_no_duplicates(current_piece, point);
        segment_id = operation.seg_id;
    }

    template
    <
        typename OutputIterator,
        typename LineStringOut,
        typename LineString,
        typename Point,
        typename Operation,
        typename SideStrategy,
        typename RobustPolicy
    >
    static inline void leave(LineStringOut& current_piece,
                LineString const& linestring,
                segment_identifier& segment_id,
                signed_size_type index, Point const& point,
                Operation const& ,
                SideStrategy const& strategy,
                RobustPolicy const& robust_policy,
                OutputIterator& out)
    {
        // On leave, copy all segments from starting point, append the intersection point
        // and add the output piece
        detail::copy_segments::copy_segments_linestring
            <
                false, RemoveSpikes
            >::apply(linestring, segment_id, index, strategy, robust_policy, current_piece);
        detail::overlay::append_no_duplicates(current_piece, point);
        if (::boost::size(current_piece) > 1)
        {
            *out++ = current_piece;
        }

        geometry::clear(current_piece);
    }

    template
    <
        typename OutputIterator,
        typename LineStringOut,
        typename LineString,
        typename Point,
        typename Operation
    >
    static inline void isolated_point(LineStringOut&,
                LineString const&,
                segment_identifier&,
                signed_size_type, Point const& point,
                Operation const& , OutputIterator& out)
    {
        LineStringOut isolated_point_ls;
        geometry::append(isolated_point_ls, point);

#ifndef BOOST_GEOMETRY_ALLOW_ONE_POINT_LINESTRINGS
        geometry::append(isolated_point_ls, point);
#endif // BOOST_GEOMETRY_ALLOW_ONE_POINT_LINESTRINGS

        *out++ = isolated_point_ls;
    }

    static inline bool is_entered(bool entered)
    {
        return entered;
    }

    static inline bool included(int inside_value)
    {
        return inside_value >= 0; // covered_by
    }

};

// Specialization for difference, which reverses these actions
template <bool RemoveSpikes>
struct action_selector<overlay_difference, RemoveSpikes>
{
    typedef action_selector<overlay_intersection, RemoveSpikes> normal_action;

    template
    <
        typename OutputIterator,
        typename LineStringOut,
        typename LineString,
        typename Point,
        typename Operation,
        typename SideStrategy,
        typename RobustPolicy
    >
    static inline void enter(LineStringOut& current_piece,
                LineString const& linestring,
                segment_identifier& segment_id,
                signed_size_type index, Point const& point,
                Operation const& operation,
                SideStrategy const& strategy,
                RobustPolicy const& robust_policy,
                OutputIterator& out)
    {
        normal_action::leave(current_piece, linestring, segment_id, index,
                    point, operation, strategy, robust_policy, out);
    }

    template
    <
        typename OutputIterator,
        typename LineStringOut,
        typename LineString,
        typename Point,
        typename Operation,
        typename SideStrategy,
        typename RobustPolicy
    >
    static inline void leave(LineStringOut& current_piece,
                LineString const& linestring,
                segment_identifier& segment_id,
                signed_size_type index, Point const& point,
                Operation const& operation,
                SideStrategy const& strategy,
                RobustPolicy const& robust_policy,
                OutputIterator& out)
    {
        normal_action::enter(current_piece, linestring, segment_id, index,
                    point, operation, strategy, robust_policy, out);
    }

    template
    <
        typename OutputIterator,
        typename LineStringOut,
        typename LineString,
        typename Point,
        typename Operation
    >
    static inline void isolated_point(LineStringOut&,
                LineString const&,
                segment_identifier&,
                signed_size_type, Point const&,
                Operation const&, OutputIterator&)
    {
    }

    static inline bool is_entered(bool entered)
    {
        return ! normal_action::is_entered(entered);
    }

    static inline bool included(int inside_value)
    {
        return ! normal_action::included(inside_value);
    }

};

}

/*!
\brief Follows a linestring from intersection point to intersection point, outputting which
    is inside, or outside, a ring or polygon
\ingroup overlay
 */
template
<
    typename LineStringOut,
    typename LineString,
    typename Polygon,
    overlay_type OverlayType,
    bool RemoveSpikes = true
>
class follow
{

    template <typename Turn>
    struct sort_on_segment
    {
        // In case of turn point at the same location, we want to have continue/blocked LAST
        // because that should be followed (intersection) or skipped (difference).
        inline int operation_order(Turn const& turn) const
        {
            operation_type const& operation = turn.operations[0].operation;
            switch(operation)
            {
                case operation_opposite : return 0;
                case operation_none : return 0;
                case operation_union : return 1;
                case operation_intersection : return 2;
                case operation_blocked : return 3;
                case operation_continue : return 4;
            }
            return -1;
        };

        inline bool use_operation(Turn const& left, Turn const& right) const
        {
            // If they are the same, OK.
            return operation_order(left) < operation_order(right);
        }

        inline bool use_distance(Turn const& left, Turn const& right) const
        {
            return left.operations[0].fraction == right.operations[0].fraction
                ? use_operation(left, right)
                : left.operations[0].fraction < right.operations[0].fraction
                ;
        }

        inline bool operator()(Turn const& left, Turn const& right) const
        {
            segment_identifier const& sl = left.operations[0].seg_id;
            segment_identifier const& sr = right.operations[0].seg_id;

            return sl == sr
                ? use_distance(left, right)
                : sl < sr
                ;

        }
    };



public :

    static inline bool included(int inside_value)
    {
        return following::action_selector
            <
                OverlayType, RemoveSpikes
            >::included(inside_value);
    }

    template
    <
        typename Turns,
        typename OutputIterator,
        typename RobustPolicy,
        typename Strategy
    >
    static inline OutputIterator apply(LineString const& linestring, Polygon const& polygon,
                detail::overlay::operation_type ,  // TODO: this parameter might be redundant
                Turns& turns,
                RobustPolicy const& robust_policy,
                OutputIterator out,
                Strategy const& strategy)
    {
        typedef typename boost::range_iterator<Turns>::type turn_iterator;
        typedef typename boost::range_value<Turns>::type turn_type;
        typedef typename boost::range_iterator
            <
                typename turn_type::container_type
            >::type turn_operation_iterator_type;

        typedef following::action_selector<OverlayType, RemoveSpikes> action;

        typename Strategy::template point_in_geometry_strategy
            <
                LineString, Polygon
            >::type const pt_in_poly_strategy
            = strategy.template get_point_in_geometry_strategy<LineString, Polygon>();

        // Sort intersection points on segments-along-linestring, and distance
        // (like in enrich is done for poly/poly)
        std::sort(boost::begin(turns), boost::end(turns), sort_on_segment<turn_type>());

        LineStringOut current_piece;
        geometry::segment_identifier current_segment_id(0, -1, -1, -1);

        // Iterate through all intersection points (they are ordered along the line)
        bool entered = false;
        bool first = true;
        for (turn_iterator it = boost::begin(turns); it != boost::end(turns); ++it)
        {
            turn_operation_iterator_type iit = boost::begin(it->operations);

            if (following::was_entered(*it, *iit, first, linestring, polygon, pt_in_poly_strategy))
            {
                debug_traverse(*it, *iit, "-> Was entered");
                entered = true;
            }

            if (following::is_staying_inside(*it, *iit, entered, first, linestring, polygon, pt_in_poly_strategy))
            {
                debug_traverse(*it, *iit, "-> Staying inside");

                entered = true;
            }
            else if (following::is_entering(*it, *iit))
            {
                debug_traverse(*it, *iit, "-> Entering");

                entered = true;
                action::enter(current_piece, linestring, current_segment_id,
                    iit->seg_id.segment_index, it->point, *iit,
                    strategy, robust_policy,
                    out);
            }
            else if (following::is_leaving(*it, *iit, entered, first, linestring, polygon, pt_in_poly_strategy))
            {
                debug_traverse(*it, *iit, "-> Leaving");

                entered = false;
                action::leave(current_piece, linestring, current_segment_id,
                    iit->seg_id.segment_index, it->point, *iit,
                    strategy, robust_policy,
                    out);
            }
            first = false;
        }

        if (action::is_entered(entered))
        {
            detail::copy_segments::copy_segments_linestring
                <
                    false, RemoveSpikes
                >::apply(linestring,
                         current_segment_id,
                         static_cast<signed_size_type>(boost::size(linestring) - 1),
                         strategy, robust_policy,
                         current_piece);
        }

        // Output the last one, if applicable
        if (::boost::size(current_piece) > 1)
        {
            *out++ = current_piece;
        }
        return out;
    }

};


}} // namespace detail::overlay
#endif // DOXYGEN_NO_DETAIL


}} // namespace boost::geometry


#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_FOLLOW_HPP
