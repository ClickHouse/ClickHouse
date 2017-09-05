// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2007-2012 Barend Gehrels, Amsterdam, the Netherlands.

// This file was modified by Oracle on 2017.
// Modifications copyright (c) 2017 Oracle and/or its affiliates.

// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_GET_INTERSECTION_POINTS_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_GET_INTERSECTION_POINTS_HPP


#include <cstddef>

#include <boost/mpl/if.hpp>
#include <boost/range.hpp>

#include <boost/geometry/algorithms/convert.hpp>
#include <boost/geometry/algorithms/detail/overlay/get_turns.hpp>

#include <boost/geometry/geometries/segment.hpp>

#include <boost/geometry/policies/robustness/robust_point_type.hpp>

namespace boost { namespace geometry
{


#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace get_intersection_points
{


template
<
    typename Point1,
    typename Point2,
    typename TurnInfo
>
struct get_turn_without_info
{
    template <typename Strategy, typename RobustPolicy, typename OutputIterator>
    static inline OutputIterator apply(
                Point1 const& pi, Point1 const& pj, Point1 const& /*pk*/,
                Point2 const& qi, Point2 const& qj, Point2 const& /*qk*/,
                bool /*is_p_first*/, bool /*is_p_last*/,
                bool /*is_q_first*/, bool /*is_q_last*/,
                TurnInfo const& ,
                Strategy const& strategy,
                RobustPolicy const& robust_policy,
                OutputIterator out)
    {
        typedef typename TurnInfo::point_type turn_point_type;

        typedef policies::relate::segments_intersection_points
            <
                segment_intersection_points
                    <
                        turn_point_type,
                        typename geometry::segment_ratio_type
                            <
                                turn_point_type, RobustPolicy
                            >::type
                    >
            > policy_type;

        typedef model::referring_segment<Point1 const> segment_type1;
        typedef model::referring_segment<Point2 const> segment_type2;
        segment_type1 p1(pi, pj);
        segment_type2 q1(qi, qj);

        typedef typename geometry::robust_point_type
            <
                Point1, RobustPolicy
            >::type robust_point_type;

        robust_point_type pi_rob, pj_rob, qi_rob, qj_rob;
        geometry::recalculate(pi_rob, pi, robust_policy);
        geometry::recalculate(pj_rob, pj, robust_policy);
        geometry::recalculate(qi_rob, qi, robust_policy);
        geometry::recalculate(qj_rob, qj, robust_policy);
        typename policy_type::return_type result
            = strategy.apply(p1, q1, policy_type(), robust_policy,
                             pi_rob, pj_rob, qi_rob, qj_rob);

        for (std::size_t i = 0; i < result.count; i++)
        {
            TurnInfo tp;
            geometry::convert(result.intersections[i], tp.point);
            *out++ = tp;
        }

        return out;
    }
};

}} // namespace detail::get_intersection_points
#endif // DOXYGEN_NO_DETAIL




template
<
    typename Geometry1,
    typename Geometry2,
    typename RobustPolicy,
    typename Turns,
    typename Strategy
>
inline void get_intersection_points(Geometry1 const& geometry1,
            Geometry2 const& geometry2,
            RobustPolicy const& robust_policy,
            Turns& turns,
            Strategy const& strategy)
{
    concepts::check_concepts_and_equal_dimensions<Geometry1 const, Geometry2 const>();

    typedef detail::get_intersection_points::get_turn_without_info
                        <
                            typename point_type<Geometry1>::type,
                            typename point_type<Geometry2>::type,
                            typename boost::range_value<Turns>::type
                        > TurnPolicy;

    detail::get_turns::no_interrupt_policy interrupt_policy;

    boost::mpl::if_c
        <
            reverse_dispatch<Geometry1, Geometry2>::type::value,
            dispatch::get_turns_reversed
            <
                typename tag<Geometry1>::type,
                typename tag<Geometry2>::type,
                Geometry1, Geometry2,
                false, false,
                TurnPolicy
            >,
            dispatch::get_turns
            <
                typename tag<Geometry1>::type,
                typename tag<Geometry2>::type,
                Geometry1, Geometry2,
                false, false,
                TurnPolicy
            >
        >::type::apply(
            0, geometry1,
            1, geometry2,
            strategy,
            robust_policy,
            turns, interrupt_policy);
}




}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_GET_INTERSECTION_POINTS_HPP
