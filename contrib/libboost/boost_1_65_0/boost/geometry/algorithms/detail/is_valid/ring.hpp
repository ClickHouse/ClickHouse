// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2014-2017, Oracle and/or its affiliates.

// Contributed and/or modified by Menelaos Karavelas, on behalf of Oracle
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Licensed under the Boost Software License version 1.0.
// http://www.boost.org/users/license.html

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_IS_VALID_RING_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_IS_VALID_RING_HPP

#include <deque>

#include <boost/core/ignore_unused.hpp>

#include <boost/geometry/core/closure.hpp>
#include <boost/geometry/core/cs.hpp>
#include <boost/geometry/core/point_order.hpp>

#include <boost/geometry/util/order_as_direction.hpp>
#include <boost/geometry/util/range.hpp>

#include <boost/geometry/algorithms/equals.hpp>

#include <boost/geometry/views/closeable_view.hpp>

#include <boost/geometry/algorithms/area.hpp>
#include <boost/geometry/algorithms/intersects.hpp>
#include <boost/geometry/algorithms/validity_failure_type.hpp>
#include <boost/geometry/algorithms/detail/num_distinct_consecutive_points.hpp>
#include <boost/geometry/algorithms/detail/is_valid/has_duplicates.hpp>
#include <boost/geometry/algorithms/detail/is_valid/has_invalid_coordinate.hpp>
#include <boost/geometry/algorithms/detail/is_valid/has_spikes.hpp>
#include <boost/geometry/algorithms/detail/is_valid/has_valid_self_turns.hpp>

#include <boost/geometry/strategies/area.hpp>

#ifdef BOOST_GEOMETRY_TEST_DEBUG
#include <boost/geometry/io/dsv/write.hpp>
#endif

namespace boost { namespace geometry
{


#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace is_valid
{


// struct to check whether a ring is topologically closed
template <typename Ring, closure_selector Closure /* open */>
struct is_topologically_closed
{
    template <typename VisitPolicy>
    static inline bool apply(Ring const&, VisitPolicy& visitor)
    {
        boost::ignore_unused(visitor);

        return visitor.template apply<no_failure>();
    }
};

template <typename Ring>
struct is_topologically_closed<Ring, closed>
{
    template <typename VisitPolicy>
    static inline bool apply(Ring const& ring, VisitPolicy& visitor)
    {
        boost::ignore_unused(visitor);

        if (geometry::equals(range::front(ring), range::back(ring)))
        {
            return visitor.template apply<no_failure>();
        }
        else
        {
            return visitor.template apply<failure_not_closed>();
        }
    }
};



template <typename ResultType, bool IsInteriorRing /* false */>
struct ring_area_predicate
{
    typedef std::greater<ResultType> type;
};

template <typename ResultType>
struct ring_area_predicate<ResultType, true>
{
    typedef std::less<ResultType> type;
};



template <typename Ring, bool IsInteriorRing>
struct is_properly_oriented
{
    template <typename VisitPolicy, typename Strategy>
    static inline bool apply(Ring const& ring, VisitPolicy& visitor,
                             Strategy const& strategy)
    {
        boost::ignore_unused(visitor);

        typedef typename point_type<Ring>::type point_type;

        typedef detail::area::ring_area
            <
                order_as_direction<geometry::point_order<Ring>::value>::value,
                geometry::closure<Ring>::value
            > ring_area_type;

        typedef typename Strategy::template area_strategy
            <
                point_type
            >::type::return_type area_result_type;

        typename ring_area_predicate
            <
                area_result_type, IsInteriorRing
            >::type predicate;

        // Check area
        area_result_type const zero = 0;
        area_result_type const area
            = ring_area_type::apply(ring,
                                    strategy.template get_area_strategy<point_type>());
        if (predicate(area, zero))
        {
            return visitor.template apply<no_failure>();
        }
        else
        {
            return visitor.template apply<failure_wrong_orientation>();
        }
    }
};



template
<
    typename Ring,
    bool CheckSelfIntersections = true,
    bool IsInteriorRing = false
>
struct is_valid_ring
{
    template <typename VisitPolicy, typename Strategy>
    static inline bool apply(Ring const& ring, VisitPolicy& visitor,
                             Strategy const& strategy)
    {
        // return invalid if any of the following condition holds:
        // (a) the ring's point coordinates are not invalid (e.g., NaN)
        // (b) the ring's size is below the minimal one
        // (c) the ring consists of at most two distinct points
        // (d) the ring is not topologically closed
        // (e) the ring has spikes
        // (f) the ring has duplicate points (if AllowDuplicates is false)
        // (g) the boundary of the ring has self-intersections
        // (h) the order of the points is inconsistent with the defined order
        //
        // Note: no need to check if the area is zero. If this is the
        // case, then the ring must have at least two spikes, which is
        // checked by condition (d).

        if (has_invalid_coordinate<Ring>::apply(ring, visitor))
        {
            return false;
        }

        closure_selector const closure = geometry::closure<Ring>::value;
        typedef typename closeable_view<Ring const, closure>::type view_type;

        if (boost::size(ring)
            < core_detail::closure::minimum_ring_size<closure>::value)
        {
            return visitor.template apply<failure_few_points>();
        }

        view_type const view(ring);
        if (detail::num_distinct_consecutive_points
                <
                    view_type, 4u, true,
                    not_equal_to<typename point_type<Ring>::type>
                >::apply(view)
            < 4u)
        {
            return
                visitor.template apply<failure_wrong_topological_dimension>();
        }

        return
            is_topologically_closed<Ring, closure>::apply(ring, visitor)
            && ! has_duplicates<Ring, closure>::apply(ring, visitor)
            && ! has_spikes<Ring, closure>::apply(ring, visitor, strategy.get_side_strategy())
            && (! CheckSelfIntersections
                || has_valid_self_turns<Ring>::apply(ring, visitor, strategy))
            && is_properly_oriented<Ring, IsInteriorRing>::apply(ring, visitor, strategy);
    }
};


}} // namespace detail::is_valid
#endif // DOXYGEN_NO_DETAIL



#ifndef DOXYGEN_NO_DISPATCH
namespace dispatch
{

// A Ring is a Polygon with exterior boundary only.
// The Ring's boundary must be a LinearRing (see OGC 06-103-r4,
// 6.1.7.1, for the definition of LinearRing)
//
// Reference (for polygon validity): OGC 06-103r4 (6.1.11.1)
template <typename Ring, bool AllowEmptyMultiGeometries>
struct is_valid
    <
        Ring, ring_tag, AllowEmptyMultiGeometries
    > : detail::is_valid::is_valid_ring<Ring>
{};


} // namespace dispatch
#endif // DOXYGEN_NO_DISPATCH


}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_IS_VALID_RING_HPP
