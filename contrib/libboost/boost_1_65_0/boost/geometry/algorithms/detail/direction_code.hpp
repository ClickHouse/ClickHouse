// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2015 Barend Gehrels, Amsterdam, the Netherlands.

// This file was modified by Oracle on 2015, 2017.
// Modifications copyright (c) 2015-2017 Oracle and/or its affiliates.

// Contributed and/or modified by Menelaos Karavelas, on behalf of Oracle
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_DIRECITON_CODE_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_DIRECITON_CODE_HPP


#include <boost/geometry/core/access.hpp>
#include <boost/geometry/util/math.hpp>
#include <boost/geometry/util/select_coordinate_type.hpp>
#include <boost/geometry/util/normalize_spheroidal_coordinates.hpp>

#include <boost/mpl/assert.hpp>


namespace boost { namespace geometry
{


#ifndef DOXYGEN_NO_DETAIL
namespace detail
{


// TODO: remove
template <std::size_t Index, typename Point1, typename Point2>
inline int sign_of_difference(Point1 const& point1, Point2 const& point2)
{
    return
        math::equals(geometry::get<Index>(point1), geometry::get<Index>(point2))
        ?
        0
        :
        (geometry::get<Index>(point1) > geometry::get<Index>(point2) ? 1 : -1);
}


template <typename Point, typename CSTag = typename cs_tag<Point>::type>
struct direction_code_impl
{
    BOOST_MPL_ASSERT_MSG((false), NOT_IMPLEMENTED_FOR_THIS_CS, (CSTag));
};

template <typename Point>
struct direction_code_impl<Point, cartesian_tag>
{
    template <typename Point1, typename Point2>
    static inline int apply(Point1 const& segment_a, Point1 const& segment_b,
                            Point2 const& p)
    {
        typedef typename geometry::select_coordinate_type
            <
                Point1, Point2
            >::type calc_t;

        if ( (math::equals(geometry::get<0>(segment_b), geometry::get<0>(segment_a))
           && math::equals(geometry::get<1>(segment_b), geometry::get<1>(segment_a)))
          || (math::equals(geometry::get<0>(segment_b), geometry::get<0>(p))
           && math::equals(geometry::get<1>(segment_b), geometry::get<1>(p))) )
        {
            return 0;
        }

        calc_t x1 = geometry::get<0>(segment_b) - geometry::get<0>(segment_a);
        calc_t y1 = geometry::get<1>(segment_b) - geometry::get<1>(segment_a);
        calc_t x2 = geometry::get<0>(segment_b) - geometry::get<0>(p);
        calc_t y2 = geometry::get<1>(segment_b) - geometry::get<1>(p);

        calc_t ax = (std::min)(math::abs(x1), math::abs(x2));
        calc_t ay = (std::min)(math::abs(y1), math::abs(y2));

        int s1 = 0, s2 = 0;
        if (ax >= ay)
        {
            s1 = x1 > 0 ? 1 : -1;
            s2 = x2 > 0 ? 1 : -1;
        }
        else
        {
            s1 = y1 > 0 ? 1 : -1;
            s2 = y2 > 0 ? 1 : -1;
        }

        return s1 == s2 ? -1 : 1;
    }
};

template <typename Point>
struct direction_code_impl<Point, spherical_equatorial_tag>
{
    template <typename Point1, typename Point2>
    static inline int apply(Point1 const& segment_a, Point1 const& segment_b,
                            Point2 const& p)
    {
        typedef typename coordinate_type<Point1>::type coord1_t;
        typedef typename coordinate_type<Point2>::type coord2_t;
        typedef typename coordinate_system<Point1>::type::units units_t;
        typedef typename coordinate_system<Point2>::type::units units2_t;
        BOOST_MPL_ASSERT_MSG((boost::is_same<units_t, units2_t>::value),
                             NOT_IMPLEMENTED_FOR_DIFFERENT_UNITS,
                             (units_t, units2_t));

        typedef typename geometry::select_coordinate_type <Point1, Point2>::type calc_t;
        typedef math::detail::constants_on_spheroid<coord1_t, units_t> constants1;
        typedef math::detail::constants_on_spheroid<coord2_t, units_t> constants2;
        typedef math::detail::constants_on_spheroid<calc_t, units_t> constants;

        coord1_t const a0 = geometry::get<0>(segment_a);
        coord1_t const a1 = geometry::get<1>(segment_a);
        coord1_t const b0 = geometry::get<0>(segment_b);
        coord1_t const b1 = geometry::get<1>(segment_b);
        coord2_t const p0 = geometry::get<0>(p);
        coord2_t const p1 = geometry::get<1>(p);
        coord1_t const pi_half1 = constants1::max_latitude();
        coord2_t const pi_half2 = constants2::max_latitude();
        calc_t const pi = constants::half_period();
        calc_t const pi_half = constants::max_latitude();
        calc_t const c0 = 0;
        
        if ( (math::equals(b0, a0) && math::equals(b1, a1))
          || (math::equals(b0, p0) && math::equals(b1, p1)) )
        {
            return 0;
        }

        bool const is_a_pole = math::equals(pi_half1, math::abs(a1));
        bool const is_b_pole = math::equals(pi_half1, math::abs(b1));
        bool const is_p_pole = math::equals(pi_half2, math::abs(p1));

        if ( is_b_pole && ((is_a_pole && math::sign(b1) == math::sign(a1))
                        || (is_p_pole && math::sign(b1) == math::sign(p1))) )
        {
            return 0;
        }

        // NOTE: as opposed to the implementation for cartesian CS
        // here point b is the origin

        calc_t const dlon1 = math::longitude_distance_signed<units_t>(b0, a0);
        calc_t const dlon2 = math::longitude_distance_signed<units_t>(b0, p0);

        bool is_antilon1 = false, is_antilon2 = false;
        calc_t const dlat1 = latitude_distance_signed(b1, a1, dlon1, pi, is_antilon1);
        calc_t const dlat2 = latitude_distance_signed(b1, p1, dlon2, pi, is_antilon2);

        calc_t mx = is_a_pole || is_b_pole || is_p_pole ?
                    c0 :
                    (std::min)(is_antilon1 ? c0 : math::abs(dlon1),
                               is_antilon2 ? c0 : math::abs(dlon2));
        calc_t my = (std::min)(math::abs(dlat1),
                               math::abs(dlat2));

        int s1 = 0, s2 = 0;
        if (mx >= my)
        {
            s1 = dlon1 > 0 ? 1 : -1;
            s2 = dlon2 > 0 ? 1 : -1;
        }
        else
        {
            s1 = dlat1 > 0 ? 1 : -1;
            s2 = dlat2 > 0 ? 1 : -1;
        }

        return s1 == s2 ? -1 : 1;
    }

    template <typename T>
    static inline T latitude_distance_signed(T const& lat1, T const& lat2, T const& lon_ds, T const& pi, bool & is_antilon)
    {
        T const c0 = 0;

        T res = lat2 - lat1;

        is_antilon = math::equals(math::abs(lon_ds), pi);
        if (is_antilon)
        {
            res = lat2 + lat1;
            if (res >= c0)
                res = pi - res;
            else
                res = -pi - res;
        }

        return res;
    }
};

template <typename Point>
struct direction_code_impl<Point, spherical_polar_tag>
{
    template <typename Point1, typename Point2>
    static inline int apply(Point1 segment_a, Point1 segment_b,
                            Point2 p)
    {
        typedef math::detail::constants_on_spheroid
            <
                typename coordinate_type<Point1>::type,
                typename coordinate_system<Point1>::type::units
            > constants1;
        typedef math::detail::constants_on_spheroid
            <
                typename coordinate_type<Point2>::type,
                typename coordinate_system<Point2>::type::units
            > constants2;

        geometry::set<1>(segment_a,
            constants1::max_latitude() - geometry::get<1>(segment_a));
        geometry::set<1>(segment_b,
            constants1::max_latitude() - geometry::get<1>(segment_b));
        geometry::set<1>(p,
            constants2::max_latitude() - geometry::get<1>(p));

        return direction_code_impl
                <
                    Point, spherical_equatorial_tag
                >::apply(segment_a, segment_b, p);
    }
};

template <typename Point>
struct direction_code_impl<Point, geographic_tag>
    : direction_code_impl<Point, spherical_equatorial_tag>
{};

// Gives sense of direction for point p, collinear w.r.t. segment (a,b)
// Returns -1 if p goes backward w.r.t (a,b), so goes from b in direction of a
// Returns 1 if p goes forward, so extends (a,b)
// Returns 0 if p is equal with b, or if (a,b) is degenerate
// Note that it does not do any collinearity test, that should be done before
template <typename Point1, typename Point2>
inline int direction_code(Point1 const& segment_a, Point1 const& segment_b,
                          Point2 const& p)
{
    return direction_code_impl<Point1>::apply(segment_a, segment_b, p);
}


} // namespace detail
#endif //DOXYGEN_NO_DETAIL


}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_DIRECITON_CODE_HPP
