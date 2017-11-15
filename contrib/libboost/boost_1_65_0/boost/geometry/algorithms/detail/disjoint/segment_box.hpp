// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2007-2014 Barend Gehrels, Amsterdam, the Netherlands.
// Copyright (c) 2008-2014 Bruno Lalande, Paris, France.
// Copyright (c) 2009-2014 Mateusz Loskot, London, UK.
// Copyright (c) 2013-2014 Adam Wulkiewicz, Lodz, Poland.

// This file was modified by Oracle on 2013-2017.
// Modifications copyright (c) 2013-2017, Oracle and/or its affiliates.

// Contributed and/or modified by Vissarion Fysikopoulos, on behalf of Oracle
// Contributed and/or modified by Menelaos Karavelas, on behalf of Oracle
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Parts of Boost.Geometry are redesigned from Geodan's Geographic Library
// (geolib/GGL), copyright (c) 1995-2010 Geodan, Amsterdam, the Netherlands.

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_DISJOINT_SEGMENT_BOX_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_DISJOINT_SEGMENT_BOX_HPP

#include <cstddef>

#include <boost/geometry/core/tags.hpp>
#include <boost/geometry/core/radian_access.hpp>

#include <boost/geometry/algorithms/detail/assign_indexed_point.hpp>
#include <boost/geometry/algorithms/detail/disjoint/point_box.hpp>
#include <boost/geometry/algorithms/detail/disjoint/box_box.hpp>
#include <boost/geometry/algorithms/detail/envelope/segment.hpp>
#include <boost/geometry/algorithms/detail/normalize.hpp>
#include <boost/geometry/algorithms/dispatch/disjoint.hpp>

#include <boost/geometry/formulas/vertex_longitude.hpp>

#include <boost/geometry/geometries/box.hpp>

namespace boost { namespace geometry
{


#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace disjoint
{

template <typename CS_Tag>
struct disjoint_segment_box_sphere_or_spheroid
{
private:

    template <typename CT>
    static inline void swap(CT& lon1,
                            CT& lat1,
                            CT& lon2,
                            CT& lat2)
    {
        std::swap(lon1, lon2);
        std::swap(lat1, lat2);
    }


public:

    template <typename Segment, typename Box, typename Strategy>
    static inline bool apply(Segment const& segment,
                             Box const& box,
                             Strategy const& azimuth_strategy)
    {
        assert_dimension_equal<Segment, Box>();

        typedef typename point_type<Segment>::type segment_point_type;
        typedef typename cs_tag<Segment>::type segment_cs_type;

        segment_point_type p0, p1;
        geometry::detail::assign_point_from_index<0>(segment, p0);
        geometry::detail::assign_point_from_index<1>(segment, p1);

        // Simplest cases first

        // Case 1: if box contains one of segment's endpoints then they are not disjoint
        if (! disjoint_point_box(p0, box) || ! disjoint_point_box(p1, box))
        {
            return false;
        }

        // Case 2: disjoint if bounding boxes are disjoint

        typedef typename coordinate_type<segment_point_type>::type CT;

        segment_point_type p0_normalized =
                geometry::detail::return_normalized<segment_point_type>(p0);
        segment_point_type p1_normalized =
                geometry::detail::return_normalized<segment_point_type>(p1);

        CT lon1 = geometry::get_as_radian<0>(p0_normalized);
        CT lat1 = geometry::get_as_radian<1>(p0_normalized);
        CT lon2 = geometry::get_as_radian<0>(p1_normalized);
        CT lat2 = geometry::get_as_radian<1>(p1_normalized);

        if (lon1 > lon2)
        {
            swap(lon1, lat1, lon2, lat2);
        }

        //Compute alp1 outside envelope and pass it to envelope_segment_impl
        //in order for it to be used later in the algorithm
        CT alp1;

        azimuth_strategy.apply(lon1, lat1, lon2, lat2, alp1);

        geometry::model::box<segment_point_type> box_seg;

        geometry::detail::envelope::envelope_segment_impl<segment_cs_type>
                ::template apply<geometry::radian>(lon1, lat1,
                                                   lon2, lat2,
                                                   box_seg,
                                                   azimuth_strategy,
                                                   alp1);
        if (disjoint_box_box(box, box_seg))
        {
            return true;
        }

        // Case 3: test intersection by comparing angles

        CT a_b0, a_b1, a_b2, a_b3;

        CT b_lon_min = geometry::get_as_radian<geometry::min_corner, 0>(box);
        CT b_lat_min = geometry::get_as_radian<geometry::min_corner, 1>(box);
        CT b_lon_max = geometry::get_as_radian<geometry::max_corner, 0>(box);
        CT b_lat_max = geometry::get_as_radian<geometry::max_corner, 1>(box);

        azimuth_strategy.apply(lon1, lat1, b_lon_min, b_lat_min, a_b0);
        azimuth_strategy.apply(lon1, lat1, b_lon_max, b_lat_min, a_b1);
        azimuth_strategy.apply(lon1, lat1, b_lon_min, b_lat_max, a_b2);
        azimuth_strategy.apply(lon1, lat1, b_lon_max, b_lat_max, a_b3);

        bool b0 = alp1 > a_b0;
        bool b1 = alp1 > a_b1;
        bool b2 = alp1 > a_b2;
        bool b3 = alp1 > a_b3;

        // if not all box points on the same side of the segment then
        // there is an intersection
        if (!(b0 && b1 && b2 && b3) && (b0 || b1 || b2 || b3))
        {
            return false;
        }

        // Case 4: The only intersection case not covered above is when all four
        // points of the box are above (below) the segment in northern (southern)
        // hemisphere. Then we have to compute the vertex of the segment

        CT vertex_lat;
        CT lat_sum = lat1 + lat2;

        if ((b0 && b1 && b2 && b3 && lat_sum > CT(0))
                || (!(b0 && b1 && b2 && b3) && lat_sum < CT(0)))
        {
            CT b_lat_below; //latitude of box closest to equator

            if (lat_sum > CT(0))
            {
                vertex_lat = geometry::get_as_radian<geometry::max_corner, 1>(box_seg);
                b_lat_below = b_lat_min;
            } else {
                vertex_lat = geometry::get_as_radian<geometry::min_corner, 1>(box_seg);
                b_lat_below = b_lat_max;
            }

            //optimization TODO: computing the spherical longitude should suffice for
            // the majority of cases
            CT vertex_lon = geometry::formula::vertex_longitude<CT, CS_Tag>
                                    ::apply(lon1, lat1,
                                            lon2, lat2,
                                            vertex_lat,
                                            alp1,
                                            azimuth_strategy);

            // Check if the vertex point is within the band defined by the
            // minimum and maximum longitude of the box; if yes, then return
            // false if the point is above the min latitude of the box; return
            // true in all other cases
            if (vertex_lon >= b_lon_min && vertex_lon <= b_lon_max
                    && std::abs(vertex_lat) > std::abs(b_lat_below))
            {
                return false;
            }
        }

        return true;
    }
};

struct disjoint_segment_box
{
    template <typename Segment, typename Box, typename Strategy>
    static inline bool apply(Segment const& segment,
                             Box const& box,
                             Strategy const& strategy)
    {
        return strategy.apply(segment, box);
    }
};

}} // namespace detail::disjoint
#endif // DOXYGEN_NO_DETAIL


#ifndef DOXYGEN_NO_DISPATCH
namespace dispatch
{


template <typename Segment, typename Box, std::size_t DimensionCount>
struct disjoint<Segment, Box, DimensionCount, segment_tag, box_tag, false>
        : detail::disjoint::disjoint_segment_box
{};


} // namespace dispatch
#endif // DOXYGEN_NO_DISPATCH


}} // namespace boost::geometry


#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_DISJOINT_SEGMENT_BOX_HPP
