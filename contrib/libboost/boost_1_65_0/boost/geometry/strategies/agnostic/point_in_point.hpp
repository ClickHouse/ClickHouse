// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2014-2017 Oracle and/or its affiliates.

// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)


#ifndef BOOST_GEOMETRY_STRATEGY_AGNOSTIC_POINT_IN_POINT_HPP
#define BOOST_GEOMETRY_STRATEGY_AGNOSTIC_POINT_IN_POINT_HPP

#include <boost/geometry/algorithms/detail/equals/point_point.hpp>

#include <boost/geometry/strategies/covered_by.hpp>
#include <boost/geometry/strategies/within.hpp>


namespace boost { namespace geometry
{

namespace strategy { namespace within
{

template
<
    typename Point1, typename Point2
>
struct point_in_point
{
    static inline bool apply(Point1 const& point1, Point2 const& point2)
    {
        return detail::equals::equals_point_point(point1, point2);
    }
};


#ifndef DOXYGEN_NO_STRATEGY_SPECIALIZATIONS

namespace services
{

template <typename PointLike1, typename PointLike2, typename Tag1, typename Tag2, typename AnyCS1, typename AnyCS2>
struct default_strategy<PointLike1, PointLike2, Tag1, Tag2, pointlike_tag, pointlike_tag, AnyCS1, AnyCS2>
{
    typedef strategy::within::point_in_point
        <
            typename point_type<PointLike1>::type,
            typename point_type<PointLike2>::type
        > type;
};


} // namespace services

#endif


}} // namespace strategy::within


#ifndef DOXYGEN_NO_STRATEGY_SPECIALIZATIONS
namespace strategy { namespace covered_by { namespace services
{

template <typename PointLike1, typename PointLike2, typename Tag1, typename Tag2, typename AnyCS1, typename AnyCS2>
struct default_strategy<PointLike1, PointLike2, Tag1, Tag2, pointlike_tag, pointlike_tag, AnyCS1, AnyCS2>
{
    typedef strategy::within::point_in_point
        <
            typename point_type<PointLike1>::type,
            typename point_type<PointLike2>::type
        > type;
};

}}} // namespace strategy::covered_by::services
#endif


}} // namespace boost::geometry


#endif // BOOST_GEOMETRY_STRATEGY_AGNOSTIC_POINT_IN_POINT_HPP
