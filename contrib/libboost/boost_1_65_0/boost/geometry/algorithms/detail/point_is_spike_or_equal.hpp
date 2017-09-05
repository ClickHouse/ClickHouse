// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2007-2015 Barend Gehrels, Amsterdam, the Netherlands.
// Copyright (c) 2008-2015 Bruno Lalande, Paris, France.
// Copyright (c) 2009-2015 Mateusz Loskot, London, UK.
// Copyright (c) 2013-2015 Adam Wulkiewicz, Lodz, Poland.

// This file was modified by Oracle on 2015, 2017.
// Modifications copyright (c) 2015-2017 Oracle and/or its affiliates.

// Contributed and/or modified by Menelaos Karavelas, on behalf of Oracle
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_POINT_IS_EQUAL_OR_SPIKE_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_POINT_IS_EQUAL_OR_SPIKE_HPP

#include <boost/geometry/algorithms/detail/direction_code.hpp>
#include <boost/geometry/algorithms/detail/recalculate.hpp>
#include <boost/geometry/core/cs.hpp>
#include <boost/geometry/policies/robustness/robust_point_type.hpp>
#include <boost/geometry/strategies/side.hpp>
#include <boost/geometry/util/condition.hpp>
#include <boost/geometry/util/math.hpp>


namespace boost { namespace geometry
{


#ifndef DOXYGEN_NO_DETAIL
namespace detail
{

template <typename Point1, typename Point2, typename Point3>
inline bool collinear_point_is_spike_or_equal(Point1 const& last_point,
                                                Point2 const& segment_a,
                                                Point3 const& segment_b)
{
    // Check if segment is equal
    int const sgn_x1 = sign_of_difference<0>(last_point, segment_b);
    int const sgn_y1 = sign_of_difference<1>(last_point, segment_b);
    if (sgn_x1 == 0 && sgn_y1 == 0)
    {
        return true;
    }

    // Check if segment moves forward
    int const sgn_x2 = sign_of_difference<0>(segment_b, segment_a);
    int const sgn_y2 = sign_of_difference<1>(segment_b, segment_a);

    return sgn_x1 != sgn_x2 || sgn_y1 != sgn_y2;
}

// Checks if a point ("last_point") causes a spike w.r.t.
// the specified two other points (segment_a, segment_b)
//
//  x-------x------x
//  a       lp     b
//
// Above, lp generates a spike w.r.t. segment(a,b)
// So specify last point first, then (a,b)
// The segment's orientation does matter: if lp is to the right of b
// no spike is reported
template
<
    typename Point1, typename Point2, typename Point3,
    typename SideStrategy
>
static inline bool point_is_spike_or_equal(Point1 const& last_point, // prev | back
                                           Point2 const& segment_a,  // next | back - 2
                                           Point3 const& segment_b,  // curr | back - 1 | spike's vertex
                                           SideStrategy const& strategy)
{
    int const side = strategy.apply(segment_a, segment_b, last_point);
    if (side == 0)
    {
        // Last point is collinear w.r.t previous segment.
#ifdef BOOST_GEOMETRY_ENABLE_POINT_IS_SPIKE_OR_EQUAL_TEST
        bool r1 = collinear_point_is_spike_or_equal(last_point, segment_a, segment_b);
        bool r2 = direction_code(segment_a, segment_b, last_point) < 1;
        if (r1 != r2)
            std::cout << "spike detection failure with: " << r1 << " " << r2 << std::endl;
        return r2;
#else
        return direction_code(segment_a, segment_b, last_point) < 1;
#endif
    }
    return false;
}

template
<
    typename Point1,
    typename Point2,
    typename Point3,
    typename SideStrategy,
    typename RobustPolicy
>
static inline bool point_is_spike_or_equal(Point1 const& last_point,
            Point2 const& segment_a,
            Point3 const& segment_b,
            SideStrategy const& strategy,
            RobustPolicy const& robust_policy)
{
    if (point_is_spike_or_equal(last_point, segment_a, segment_b, strategy))
    {
        return true;
    }

    if (BOOST_GEOMETRY_CONDITION(! RobustPolicy::enabled))
    {
        return false;
    }

    // Try using specified robust policy
    typedef typename geometry::robust_point_type
    <
        Point1,
        RobustPolicy
    >::type robust_point_type;

    robust_point_type last_point_rob, segment_a_rob, segment_b_rob;
    geometry::recalculate(last_point_rob, last_point, robust_policy);
    geometry::recalculate(segment_a_rob, segment_a, robust_policy);
    geometry::recalculate(segment_b_rob, segment_b, robust_policy);

    return point_is_spike_or_equal
        (
            last_point_rob,
            segment_a_rob,
            segment_b_rob,
            strategy
        );
}


} // namespace detail
#endif

}} // namespace boost::geometry


#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_POINT_IS_EQUAL_OR_SPIKE_HPP
