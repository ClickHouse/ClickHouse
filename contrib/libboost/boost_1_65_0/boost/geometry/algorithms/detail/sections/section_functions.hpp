// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2015 Barend Gehrels, Amsterdam, the Netherlands.

// This file was modified by Oracle on 2015, 2017.
// Modifications copyright (c) 2015-2017, Oracle and/or its affiliates.

// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_SECTIONS_FUNCTIONS_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_SECTIONS_FUNCTIONS_HPP


#include <boost/geometry/core/access.hpp>
#include <boost/geometry/algorithms/detail/recalculate.hpp>
#include <boost/geometry/policies/robustness/robust_point_type.hpp>


namespace boost { namespace geometry
{

#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace section
{

template
<
    std::size_t Dimension,
    typename Geometry,
    typename CastedCSTag = typename tag_cast
                            <
                                typename cs_tag<Geometry>::type,
                                spherical_tag
                            >::type
>
struct preceding_check
{
    template <typename Point, typename Box>
    static inline bool apply(int dir, Point const& point, Box const& /*point_box*/, Box const& other_box)
    {
        return (dir == 1  && get<Dimension>(point) < get<min_corner, Dimension>(other_box))
            || (dir == -1 && get<Dimension>(point) > get<max_corner, Dimension>(other_box));
    }
};

template <typename Geometry>
struct preceding_check<0, Geometry, spherical_tag>
{
    template <typename Point, typename Box>
    static inline bool apply(int dir, Point const& point, Box const& point_box, Box const& other_box)
    {
        typedef typename select_coordinate_type
            <
                Point, Box
            >::type calc_t;
        typedef typename coordinate_system<Point>::type::units units_t;

        calc_t const c0 = 0;

        if (dir == 1)
        {
            calc_t const diff_min = math::longitude_distance_signed
                                        <
                                            units_t, calc_t
                                        >(get<min_corner, 0>(other_box), get<0>(point));

            calc_t const diff_min_min = math::longitude_distance_signed
                                        <
                                            units_t, calc_t
                                        >(get<min_corner, 0>(other_box), get<min_corner, 0>(point_box));

            return diff_min < c0 && diff_min_min <= c0 && diff_min_min <= diff_min;
        }
        else if (dir == -1)
        {
            calc_t const diff_max = math::longitude_distance_signed
                                        <
                                            units_t, calc_t
                                        >(get<max_corner, 0>(other_box), get<0>(point));

            calc_t const diff_max_max = math::longitude_distance_signed
                                        <
                                            units_t, calc_t
                                        >(get<max_corner, 0>(other_box), get<max_corner, 0>(point_box));

            return diff_max > c0 && diff_max_max >= c0 && diff_max <= diff_max_max;
        }

        return false;
    }
};


template
<
    std::size_t Dimension,
    typename Point,
    typename RobustBox,
    typename RobustPolicy
>
static inline bool preceding(int dir,
                             Point const& point,
                             RobustBox const& point_robust_box,
                             RobustBox const& other_robust_box,
                             RobustPolicy const& robust_policy)
{
    typename geometry::robust_point_type<Point, RobustPolicy>::type robust_point;
    geometry::recalculate(robust_point, point, robust_policy);
    return preceding_check<Dimension, Point>::apply(dir, robust_point, point_robust_box, other_robust_box);
}

template
<
    std::size_t Dimension,
    typename Point,
    typename RobustBox,
    typename RobustPolicy
>
static inline bool exceeding(int dir,
                             Point const& point,
                             RobustBox const& point_robust_box,
                             RobustBox const& other_robust_box,
                             RobustPolicy const& robust_policy)
{
    return preceding<Dimension>(-dir, point, point_robust_box, other_robust_box, robust_policy);
}


}} // namespace detail::section
#endif


}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_SECTIONS_FUNCTIONS_HPP
