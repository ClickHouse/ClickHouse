// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2015, Oracle and/or its affiliates.

// Contributed and/or modified by Menelaos Karavelas, on behalf of Oracle

// Licensed under the Boost Software License version 1.0.
// http://www.boost.org/users/license.html

#ifndef BOOST_GEOMETRY_VIEWS_DETAIL_TWO_DIMENSIONAL_VIEW_HPP
#define BOOST_GEOMETRY_VIEWS_DETAIL_TWO_DIMENSIONAL_VIEW_HPP

#include <cstddef>

#include <boost/mpl/assert.hpp>
#include <boost/mpl/int.hpp>

#include <boost/geometry/core/access.hpp>
#include <boost/geometry/core/coordinate_type.hpp>
#include <boost/geometry/core/coordinate_system.hpp>
#include <boost/geometry/core/coordinate_dimension.hpp>
#include <boost/geometry/core/point_type.hpp>
#include <boost/geometry/core/tag.hpp>
#include <boost/geometry/core/tags.hpp>

#include <boost/geometry/algorithms/not_implemented.hpp>


namespace boost { namespace geometry
{

#ifndef DOXYGEN_NO_DETAIL
namespace detail
{

template
<
    typename Geometry,
    std::size_t Dimension1 = 0,
    std::size_t Dimension2 = 1,
    typename Tag = typename tag<Geometry>::type
>
struct two_dimensional_view
    : not_implemented<Tag>
{};


// View that enables to choose two dimensions of a point and see it as
// a two-dimensional point
template <typename Point, std::size_t Dimension1, std::size_t Dimension2>
struct two_dimensional_view<Point, Dimension1, Dimension2, point_tag>
{
    BOOST_MPL_ASSERT_MSG(
        (Dimension1 < static_cast<std::size_t>(dimension<Point>::value)),
        COORDINATE_DIMENSION1_IS_LARGER_THAN_POINT_DIMENSION,
        (boost::mpl::int_<Dimension1>));

    BOOST_MPL_ASSERT_MSG(
        (Dimension2 < static_cast<std::size_t>(dimension<Point>::value)),
        COORDINATE_DIMENSION2_IS_LARGER_THAN_POINT_DIMENSION,
        (boost::mpl::int_<Dimension2>));

    two_dimensional_view(Point& point)
        : m_point(point)
    {}

    Point& m_point;
};


} // namespace detail
#endif // DOXYGEN_NO_DETAIL


#ifndef DOXYGEN_NO_TRAITS_SPECIALIZATIONS
namespace traits
{


template <typename Point, std::size_t Dimension1, std::size_t Dimension2>
struct tag
    <
        geometry::detail::two_dimensional_view
            <
                Point, Dimension1, Dimension2, point_tag
            >
    >
{
    typedef point_tag type;
};

template <typename Point, std::size_t Dimension1, std::size_t Dimension2>
struct coordinate_system
    <
        geometry::detail::two_dimensional_view
            <
                Point, Dimension1, Dimension2, point_tag
            >
    > : coordinate_system<typename geometry::point_type<Point>::type>
{};

template <typename Point, std::size_t Dimension1, std::size_t Dimension2>
struct coordinate_type
    <
        geometry::detail::two_dimensional_view
            <
                Point, Dimension1, Dimension2, point_tag
            >
    > : coordinate_type<typename geometry::point_type<Point>::type>
{};

template <typename Point, std::size_t Dimension1, std::size_t Dimension2>
struct dimension
    <
        geometry::detail::two_dimensional_view
            <
                Point, Dimension1, Dimension2, point_tag
            >
    > : boost::mpl::int_<2>
{};

template <typename Point, std::size_t Dimension1, std::size_t Dimension2>
struct point_type
    <
        geometry::detail::two_dimensional_view
            <
                Point, Dimension1, Dimension2, point_tag
            >
    >
{
    typedef typename geometry::point_type<Point>::type type;
};


template <typename Point, std::size_t Dimension1, std::size_t Dimension2>
struct access
    <
        geometry::detail::two_dimensional_view
            <
                Point, Dimension1, Dimension2, point_tag
            >,
        0
    >
{
    typedef typename geometry::coordinate_type<Point>::type coordinate_type;
    typedef geometry::detail::two_dimensional_view
        <
            Point, Dimension1, Dimension2, point_tag
        > view_type;

    static inline coordinate_type get(view_type const& view)
    {
        return geometry::get<Dimension1>(view.m_point);
    }

    static inline void set(view_type& view, coordinate_type const& value)
    {
        geometry::set<Dimension1>(view.m_point, value);
    }
};

template <typename Point, std::size_t Dimension1, std::size_t Dimension2>
struct access
    <
        geometry::detail::two_dimensional_view
            <
                Point, Dimension1, Dimension2, point_tag
            >,
        1
    >
{
    typedef typename geometry::coordinate_type<Point>::type coordinate_type;
    typedef geometry::detail::two_dimensional_view
        <
            Point, Dimension1, Dimension2, point_tag
        > view_type;

    static inline coordinate_type get(view_type const& view)
    {
        return geometry::get<Dimension2>(view.m_point);
    }

    static inline void set(view_type& view, coordinate_type const& value)
    {
        geometry::set<Dimension2>(view.m_point, value);
    }
};


} // namespace traits
#endif // DOXYGEN_NO_TRAITS_SPECIALIZATIONS

}} // namespace boost::geometry


#endif // BOOST_GEOMETRY_VIEWS_DETAIL_TWO_DIMENSIONAL_VIEW_HPP
