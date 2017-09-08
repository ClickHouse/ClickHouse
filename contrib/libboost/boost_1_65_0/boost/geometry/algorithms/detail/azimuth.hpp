// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2007-2012 Barend Gehrels, Amsterdam, the Netherlands.

// This file was modified by Oracle on 2014, 2016, 2017.
// Modifications copyright (c) 2014-2017, Oracle and/or its affiliates.

// Contributed and/or modified by Vissarion Fysikopoulos, on behalf of Oracle
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_AZIMUTH_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_AZIMUTH_HPP

#include <boost/geometry/core/cs.hpp>
#include <boost/geometry/core/access.hpp>
#include <boost/geometry/core/radian_access.hpp>
#include <boost/geometry/core/tags.hpp>

#include <boost/geometry/util/math.hpp>

#include <boost/geometry/algorithms/not_implemented.hpp>

#include <boost/geometry/formulas/spherical.hpp>
#include <boost/geometry/formulas/vincenty_inverse.hpp>

namespace boost { namespace geometry
{

// An azimuth is an angle between a vector/segment from origin to a point of
// interest and a reference vector. Typically north-based azimuth is used.
// North direction is used as a reference, angle is measured clockwise
// (North - 0deg, East - 90deg). For consistency in 2d cartesian CS
// the reference vector is Y axis, angle is measured clockwise.
// http://en.wikipedia.org/wiki/Azimuth

#ifndef DOXYGEN_NO_DISPATCH
namespace detail_dispatch
{

template <typename ReturnType, typename Tag>
struct azimuth
    : not_implemented<Tag>
{};

template <typename ReturnType>
struct azimuth<ReturnType, geographic_tag>
{
    template <typename P1, typename P2, typename Spheroid>
    static inline ReturnType apply(P1 const& p1, P2 const& p2, Spheroid const& spheroid)
    {
        return geometry::formula::vincenty_inverse<ReturnType, false, true>().apply
                    ( get_as_radian<0>(p1), get_as_radian<1>(p1),
                      get_as_radian<0>(p2), get_as_radian<1>(p2),
                      spheroid ).azimuth;
    }

    template <typename P1, typename P2>
    static inline ReturnType apply(P1 const& p1, P2 const& p2)
    {
        return apply(p1, p2, srs::spheroid<ReturnType>());
    }
};

template <typename ReturnType>
struct azimuth<ReturnType, spherical_equatorial_tag>
{
    template <typename P1, typename P2, typename Sphere>
    static inline ReturnType apply(P1 const& p1, P2 const& p2, Sphere const& /*unused*/)
    {
        return geometry::formula::spherical_azimuth<ReturnType, false>
                    ( get_as_radian<0>(p1), get_as_radian<1>(p1),
                      get_as_radian<0>(p2), get_as_radian<1>(p2)).azimuth;
    }

    template <typename P1, typename P2>
    static inline ReturnType apply(P1 const& p1, P2 const& p2)
    {
        return apply(p1, p2, 0); // dummy model
    }
};

template <typename ReturnType>
struct azimuth<ReturnType, spherical_polar_tag>
    : azimuth<ReturnType, spherical_equatorial_tag>
{};

template <typename ReturnType>
struct azimuth<ReturnType, cartesian_tag>
{
    template <typename P1, typename P2, typename Plane>
    static inline ReturnType apply(P1 const& p1, P2 const& p2, Plane const& /*unused*/)
    {
        ReturnType x = get<0>(p2) - get<0>(p1);
        ReturnType y = get<1>(p2) - get<1>(p1);

        // NOTE: azimuth 0 is at Y axis, increasing right
        // as in spherical/geographic where 0 is at North axis
        return atan2(x, y);
    }

    template <typename P1, typename P2>
    static inline ReturnType apply(P1 const& p1, P2 const& p2)
    {
        return apply(p1, p2, 0); // dummy model
    }
};

} // detail_dispatch
#endif // DOXYGEN_NO_DISPATCH

#ifndef DOXYGEN_NO_DETAIL
namespace detail
{

/// Calculate azimuth between two points.
/// The result is in radians.
template <typename ReturnType, typename Point1, typename Point2>
inline ReturnType azimuth(Point1 const& p1, Point2 const& p2)
{
    return detail_dispatch::azimuth
            <
                ReturnType,
                typename geometry::cs_tag<Point1>::type
            >::apply(p1, p2);
}

/// Calculate azimuth between two points.
/// The result is in radians.
template <typename ReturnType, typename Point1, typename Point2, typename Model>
inline ReturnType azimuth(Point1 const& p1, Point2 const& p2, Model const& model)
{
    return detail_dispatch::azimuth
            <
                ReturnType,
                typename geometry::cs_tag<Point1>::type
            >::apply(p1, p2, model);
}

} // namespace detail
#endif // DOXYGEN_NO_DETAIL

}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_AZIMUTH_HPP
