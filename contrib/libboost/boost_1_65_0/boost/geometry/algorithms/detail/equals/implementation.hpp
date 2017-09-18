// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2007-2015 Barend Gehrels, Amsterdam, the Netherlands.
// Copyright (c) 2008-2015 Bruno Lalande, Paris, France.
// Copyright (c) 2009-2015 Mateusz Loskot, London, UK.
// Copyright (c) 2014-2015 Adam Wulkiewicz, Lodz, Poland.

// This file was modified by Oracle on 2014, 2015, 2016, 2017.
// Modifications copyright (c) 2014-2017 Oracle and/or its affiliates.

// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle
// Contributed and/or modified by Menelaos Karavelas, on behalf of Oracle

// Parts of Boost.Geometry are redesigned from Geodan's Geographic Library
// (geolib/GGL), copyright (c) 1995-2010 Geodan, Amsterdam, the Netherlands.

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_EQUALS_IMPLEMENTATION_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_EQUALS_IMPLEMENTATION_HPP


#include <cstddef>
#include <vector>

#include <boost/range.hpp>
#include <boost/type_traits/is_base_of.hpp>

#include <boost/geometry/core/access.hpp>
#include <boost/geometry/core/tags.hpp>

#include <boost/geometry/algorithms/detail/equals/point_point.hpp>

// For trivial checks
#include <boost/geometry/algorithms/area.hpp>
#include <boost/geometry/algorithms/length.hpp>
#include <boost/geometry/util/math.hpp>
#include <boost/geometry/util/select_coordinate_type.hpp>
#include <boost/geometry/util/select_most_precise.hpp>

#include <boost/geometry/algorithms/detail/equals/collect_vectors.hpp>
#include <boost/geometry/algorithms/detail/equals/interface.hpp>
#include <boost/geometry/algorithms/detail/relate/relate_impl.hpp>
#include <boost/geometry/algorithms/relate.hpp>

#include <boost/geometry/views/detail/indexed_point_view.hpp>


namespace boost { namespace geometry
{

#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace equals
{


template
<
    std::size_t Dimension,
    std::size_t DimensionCount
>
struct point_point
{
    template <typename Point1, typename Point2, typename Strategy>
    static inline bool apply(Point1 const& point1, Point2 const& point2, Strategy const& strategy)
    {
        return ! detail::disjoint::point_point
            <
                Point1, Point2,
                Dimension, DimensionCount
            >::apply(point1, point2, strategy);
    }
};


template
<
    std::size_t Dimension,
    std::size_t DimensionCount
>
struct box_box
{
    template <typename Box1, typename Box2, typename Strategy>
    static inline bool apply(Box1 const& box1, Box2 const& box2, Strategy const& strategy)
    {
        if (!geometry::math::equals(get<min_corner, Dimension>(box1), get<min_corner, Dimension>(box2))
            || !geometry::math::equals(get<max_corner, Dimension>(box1), get<max_corner, Dimension>(box2)))
        {
            return false;
        }
        return box_box<Dimension + 1, DimensionCount>::apply(box1, box2, strategy);
    }
};

template <std::size_t DimensionCount>
struct box_box<DimensionCount, DimensionCount>
{
    template <typename Box1, typename Box2, typename Strategy>
    static inline bool apply(Box1 const& , Box2 const& , Strategy const& )
    {
        return true;
    }
};


struct segment_segment
{
    template <typename Segment1, typename Segment2, typename Strategy>
    static inline bool apply(Segment1 const& segment1, Segment2 const& segment2, Strategy const& )
    {
        return equals::equals_point_point(
                    indexed_point_view<Segment1 const, 0>(segment1),
                    indexed_point_view<Segment2 const, 0>(segment2) )
                ? equals::equals_point_point(
                    indexed_point_view<Segment1 const, 1>(segment1),
                    indexed_point_view<Segment2 const, 1>(segment2) )
                : ( equals::equals_point_point(
                        indexed_point_view<Segment1 const, 0>(segment1),
                        indexed_point_view<Segment2 const, 1>(segment2) )
                 && equals::equals_point_point(
                        indexed_point_view<Segment1 const, 1>(segment1),
                        indexed_point_view<Segment2 const, 0>(segment2) )
                  );
    }
};


struct area_check
{
    template <typename Geometry1, typename Geometry2, typename Strategy>
    static inline bool apply(Geometry1 const& geometry1,
                             Geometry2 const& geometry2,
                             Strategy const& strategy)
    {
        return geometry::math::equals(
            geometry::area(geometry1,
                           strategy.template get_area_strategy<Geometry1>()),
            geometry::area(geometry2,
                           strategy.template get_area_strategy<Geometry2>()));
    }
};


struct length_check
{
    template <typename Geometry1, typename Geometry2, typename Strategy>
    static inline bool apply(Geometry1 const& geometry1,
                             Geometry2 const& geometry2,
                             Strategy const& strategy)
    {
        return geometry::math::equals(
            geometry::length(geometry1,
                             strategy.template get_distance_strategy<Geometry1>()),
            geometry::length(geometry2,
                             strategy.template get_distance_strategy<Geometry2>()));
    }
};


template <typename Geometry1, typename Geometry2, typename IntersectionStrategy>
struct collected_vector
{
    typedef typename geometry::select_most_precise
        <
            typename select_coordinate_type
                <
                    Geometry1, Geometry2
                >::type,
            double
        >::type calculation_type;

    typedef geometry::collected_vector
        <
            calculation_type,
            Geometry1,
            typename IntersectionStrategy::side_strategy_type
        > type;
};

template <typename TrivialCheck>
struct equals_by_collection
{
    template <typename Geometry1, typename Geometry2, typename Strategy>
    static inline bool apply(Geometry1 const& geometry1,
                             Geometry2 const& geometry2,
                             Strategy const& strategy)
    {
        if (! TrivialCheck::apply(geometry1, geometry2, strategy))
        {
            return false;
        }

        typedef typename collected_vector
            <
                Geometry1, Geometry2, Strategy
            >::type collected_vector_type;

        std::vector<collected_vector_type> c1, c2;

        geometry::collect_vectors(c1, geometry1);
        geometry::collect_vectors(c2, geometry2);

        if (boost::size(c1) != boost::size(c2))
        {
            return false;
        }

        std::sort(c1.begin(), c1.end());
        std::sort(c2.begin(), c2.end());

        // Just check if these vectors are equal.
        return std::equal(c1.begin(), c1.end(), c2.begin());
    }
};

template<typename Geometry1, typename Geometry2>
struct equals_by_relate
    : detail::relate::relate_impl
        <
            detail::de9im::static_mask_equals_type,
            Geometry1,
            Geometry2
        >
{};

// If collect_vectors which is a SideStrategy-dispatched optimization
// is implemented in a way consistent with the Intersection/Side Strategy
// then collect_vectors is used, otherwise relate is used.
// NOTE: the result could be conceptually different for invalid
// geometries in different coordinate systems because collect_vectors
// and relate treat invalid geometries differently.
template<typename TrivialCheck>
struct equals_by_collection_or_relate
{
    template <typename Geometry1, typename Geometry2, typename Strategy>
    static inline bool apply(Geometry1 const& geometry1,
                             Geometry2 const& geometry2,
                             Strategy const& strategy)
    {
        typedef typename boost::is_base_of
            <
                nyi::not_implemented_tag,
                typename collected_vector
                    <
                        Geometry1, Geometry2, Strategy
                    >::type
            >::type enable_relate_type;

        return apply(geometry1, geometry2, strategy, enable_relate_type());
    }

private:
    template <typename Geometry1, typename Geometry2, typename Strategy>
    static inline bool apply(Geometry1 const& geometry1,
                             Geometry2 const& geometry2,
                             Strategy const& strategy,
                             boost::false_type /*enable_relate*/)
    {
        return equals_by_collection<TrivialCheck>::apply(geometry1, geometry2, strategy);
    }

    template <typename Geometry1, typename Geometry2, typename Strategy>
    static inline bool apply(Geometry1 const& geometry1,
                             Geometry2 const& geometry2,
                             Strategy const& strategy,
                             boost::true_type /*enable_relate*/)
    {
        return equals_by_relate<Geometry1, Geometry2>::apply(geometry1, geometry2, strategy);
    }
};


}} // namespace detail::equals
#endif // DOXYGEN_NO_DETAIL


#ifndef DOXYGEN_NO_DISPATCH
namespace dispatch
{

template <typename P1, typename P2, std::size_t DimensionCount, bool Reverse>
struct equals<P1, P2, point_tag, point_tag, DimensionCount, Reverse>
    : detail::equals::point_point<0, DimensionCount>
{};

template <typename MultiPoint1, typename MultiPoint2, std::size_t DimensionCount, bool Reverse>
struct equals<MultiPoint1, MultiPoint2, multi_point_tag, multi_point_tag, DimensionCount, Reverse>
    : detail::equals::equals_by_relate<MultiPoint1, MultiPoint2>
{};

template <typename MultiPoint, typename Point, std::size_t DimensionCount, bool Reverse>
struct equals<Point, MultiPoint, point_tag, multi_point_tag, DimensionCount, Reverse>
    : detail::equals::equals_by_relate<Point, MultiPoint>
{};

template <typename Box1, typename Box2, std::size_t DimensionCount, bool Reverse>
struct equals<Box1, Box2, box_tag, box_tag, DimensionCount, Reverse>
    : detail::equals::box_box<0, DimensionCount>
{};


template <typename Ring1, typename Ring2, bool Reverse>
struct equals<Ring1, Ring2, ring_tag, ring_tag, 2, Reverse>
    : detail::equals::equals_by_collection_or_relate<detail::equals::area_check>
{};


template <typename Polygon1, typename Polygon2, bool Reverse>
struct equals<Polygon1, Polygon2, polygon_tag, polygon_tag, 2, Reverse>
    : detail::equals::equals_by_collection_or_relate<detail::equals::area_check>
{};


template <typename Polygon, typename Ring, bool Reverse>
struct equals<Polygon, Ring, polygon_tag, ring_tag, 2, Reverse>
    : detail::equals::equals_by_collection_or_relate<detail::equals::area_check>
{};


template <typename Ring, typename Box, bool Reverse>
struct equals<Ring, Box, ring_tag, box_tag, 2, Reverse>
    : detail::equals::equals_by_collection<detail::equals::area_check>
{};


template <typename Polygon, typename Box, bool Reverse>
struct equals<Polygon, Box, polygon_tag, box_tag, 2, Reverse>
    : detail::equals::equals_by_collection<detail::equals::area_check>
{};

template <typename Segment1, typename Segment2, std::size_t DimensionCount, bool Reverse>
struct equals<Segment1, Segment2, segment_tag, segment_tag, DimensionCount, Reverse>
    : detail::equals::segment_segment
{};

template <typename LineString1, typename LineString2, bool Reverse>
struct equals<LineString1, LineString2, linestring_tag, linestring_tag, 2, Reverse>
    : detail::equals::equals_by_relate<LineString1, LineString2>
{};

template <typename LineString, typename MultiLineString, bool Reverse>
struct equals<LineString, MultiLineString, linestring_tag, multi_linestring_tag, 2, Reverse>
    : detail::equals::equals_by_relate<LineString, MultiLineString>
{};

template <typename MultiLineString1, typename MultiLineString2, bool Reverse>
struct equals<MultiLineString1, MultiLineString2, multi_linestring_tag, multi_linestring_tag, 2, Reverse>
    : detail::equals::equals_by_relate<MultiLineString1, MultiLineString2>
{};


template <typename MultiPolygon1, typename MultiPolygon2, bool Reverse>
struct equals
    <
        MultiPolygon1, MultiPolygon2,
        multi_polygon_tag, multi_polygon_tag,
        2,
        Reverse
    >
    : detail::equals::equals_by_collection_or_relate<detail::equals::area_check>
{};


template <typename Polygon, typename MultiPolygon, bool Reverse>
struct equals
    <
        Polygon, MultiPolygon,
        polygon_tag, multi_polygon_tag,
        2,
        Reverse
    >
    : detail::equals::equals_by_collection_or_relate<detail::equals::area_check>
{};

template <typename MultiPolygon, typename Ring, bool Reverse>
struct equals
    <
        MultiPolygon, Ring,
        multi_polygon_tag, ring_tag,
        2,
        Reverse
    >
    : detail::equals::equals_by_collection_or_relate<detail::equals::area_check>
{};


} // namespace dispatch
#endif // DOXYGEN_NO_DISPATCH


}} // namespace boost::geometry


#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_EQUALS_IMPLEMENTATION_HPP

