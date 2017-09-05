// Boost.Geometry Index
//
// boxes union/intersection area/volume
//
// Copyright (c) 2011-2017 Adam Wulkiewicz, Lodz, Poland.
//
// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_INDEX_DETAIL_ALGORITHMS_INTERSECTION_CONTENT_HPP
#define BOOST_GEOMETRY_INDEX_DETAIL_ALGORITHMS_INTERSECTION_CONTENT_HPP

#include <boost/geometry/algorithms/detail/disjoint/box_box.hpp>
#include <boost/geometry/algorithms/detail/overlay/intersection_box_box.hpp>

#include <boost/geometry/index/detail/algorithms/content.hpp>

namespace boost { namespace geometry { namespace index { namespace detail {

/**
 * \brief Compute the area of the intersection of b1 and b2
 */
template <typename Box>
inline typename default_content_result<Box>::type intersection_content(Box const& box1, Box const& box2)
{
    bool const intersects = ! geometry::detail::disjoint::box_box<Box, Box>::apply(box1, box2);

    if ( intersects )
    {
        Box box_intersection;
        bool const ok = geometry::detail::intersection::intersection_box_box
                            <
                                0, geometry::dimension<Box>::value
                            >::apply(box1, box2, 0, box_intersection, 0);
        if ( ok )
        {
            return index::detail::content(box_intersection);
        }
    }
    return 0;
}

}}}} // namespace boost::geometry::index::detail

#endif // BOOST_GEOMETRY_INDEX_DETAIL_ALGORITHMS_INTERSECTION_CONTENT_HPP
