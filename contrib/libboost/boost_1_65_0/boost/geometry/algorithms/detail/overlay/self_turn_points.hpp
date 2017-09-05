// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2007-2012 Barend Gehrels, Amsterdam, the Netherlands.
// Copyright (c) 2017 Adam Wulkiewicz, Lodz, Poland.

// This file was modified by Oracle on 2017.
// Modifications copyright (c) 2017 Oracle and/or its affiliates.

// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_SELF_TURN_POINTS_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_SELF_TURN_POINTS_HPP


#include <cstddef>

#include <boost/mpl/vector_c.hpp>
#include <boost/range.hpp>

#include <boost/geometry/core/access.hpp>
#include <boost/geometry/core/coordinate_dimension.hpp>
#include <boost/geometry/core/point_order.hpp>
#include <boost/geometry/core/tags.hpp>

#include <boost/geometry/geometries/concepts/check.hpp>

#include <boost/geometry/algorithms/detail/disjoint/box_box.hpp>
#include <boost/geometry/algorithms/detail/partition.hpp>
#include <boost/geometry/algorithms/detail/overlay/do_reverse.hpp>
#include <boost/geometry/algorithms/detail/overlay/get_turns.hpp>
#include <boost/geometry/algorithms/detail/sections/section_box_policies.hpp>

#include <boost/geometry/geometries/box.hpp>

#include <boost/geometry/util/condition.hpp>


namespace boost { namespace geometry
{

#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace self_get_turn_points
{

struct no_interrupt_policy
{
    static bool const enabled = false;
    static bool const has_intersections = false;


    template <typename Range>
    static inline bool apply(Range const&)
    {
        return false;
    }
};


template
<
    bool Reverse,
    typename Geometry,
    typename Turns,
    typename TurnPolicy,
    typename IntersectionStrategy,
    typename RobustPolicy,
    typename InterruptPolicy
>
struct self_section_visitor
{
    Geometry const& m_geometry;
    IntersectionStrategy const& m_intersection_strategy;
    RobustPolicy const& m_rescale_policy;
    Turns& m_turns;
    InterruptPolicy& m_interrupt_policy;
    std::size_t m_source_index;

    inline self_section_visitor(Geometry const& g,
                                IntersectionStrategy const& is,
                                RobustPolicy const& rp,
                                Turns& turns,
                                InterruptPolicy& ip,
                                std::size_t source_index)
        : m_geometry(g)
        , m_intersection_strategy(is)
        , m_rescale_policy(rp)
        , m_turns(turns)
        , m_interrupt_policy(ip)
        , m_source_index(source_index)
    {}

    template <typename Section>
    inline bool apply(Section const& sec1, Section const& sec2)
    {
        if (! detail::disjoint::disjoint_box_box(sec1.bounding_box, sec2.bounding_box)
                && ! sec1.duplicate
                && ! sec2.duplicate)
        {
            // false if interrupted
            return detail::get_turns::get_turns_in_sections
                    <
                        Geometry, Geometry,
                        Reverse, Reverse,
                        Section, Section,
                        TurnPolicy
                    >::apply(m_source_index, m_geometry, sec1,
                             m_source_index, m_geometry, sec2,
                             false,
                             m_intersection_strategy,
                             m_rescale_policy,
                             m_turns, m_interrupt_policy);
        }

        return true;
    }

};



template <bool Reverse, typename TurnPolicy>
struct get_turns
{
    template <typename Geometry, typename IntersectionStrategy, typename RobustPolicy, typename Turns, typename InterruptPolicy>
    static inline bool apply(
            Geometry const& geometry,
            IntersectionStrategy const& intersection_strategy,
            RobustPolicy const& robust_policy,
            Turns& turns,
            InterruptPolicy& interrupt_policy,
            std::size_t source_index)
    {
        typedef model::box
            <
                typename geometry::robust_point_type
                <
                    typename geometry::point_type<Geometry>::type,
                    RobustPolicy
                >::type
            > box_type;

        typedef geometry::sections<box_type, 1> sections_type;

        typedef boost::mpl::vector_c<std::size_t, 0> dimensions;

        sections_type sec;
        geometry::sectionalize<Reverse, dimensions>(geometry, robust_policy, sec,
                                                  intersection_strategy.get_envelope_strategy());

        self_section_visitor
            <
                Reverse, Geometry,
                Turns, TurnPolicy, IntersectionStrategy, RobustPolicy, InterruptPolicy
            > visitor(geometry, intersection_strategy, robust_policy, turns, interrupt_policy, source_index);

        // false if interrupted
        geometry::partition
            <
                box_type
            >::apply(sec, visitor,
                     detail::section::get_section_box(),
                     detail::section::overlaps_section_box());

        return ! interrupt_policy.has_intersections;
    }
};


}} // namespace detail::self_get_turn_points
#endif // DOXYGEN_NO_DETAIL


#ifndef DOXYGEN_NO_DISPATCH
namespace dispatch
{

template
<
    bool Reverse,
    typename GeometryTag,
    typename Geometry,
    typename TurnPolicy
>
struct self_get_turn_points
{
};


template
<
    bool Reverse,
    typename Ring,
    typename TurnPolicy
>
struct self_get_turn_points
    <
        Reverse, ring_tag, Ring,
        TurnPolicy
    >
    : detail::self_get_turn_points::get_turns<Reverse, TurnPolicy>
{};


template
<
    bool Reverse,
    typename Box,
    typename TurnPolicy
>
struct self_get_turn_points
    <
        Reverse, box_tag, Box,
        TurnPolicy
    >
{
    template <typename Strategy, typename RobustPolicy, typename Turns, typename InterruptPolicy>
    static inline bool apply(
            Box const& ,
            Strategy const& ,
            RobustPolicy const& ,
            Turns& ,
            InterruptPolicy& ,
            std::size_t)
    {
        return true;
    }
};


template
<
    bool Reverse,
    typename Polygon,
    typename TurnPolicy
>
struct self_get_turn_points
    <
        Reverse, polygon_tag, Polygon,
        TurnPolicy
    >
    : detail::self_get_turn_points::get_turns<Reverse, TurnPolicy>
{};


template
<
    bool Reverse,
    typename MultiPolygon,
    typename TurnPolicy
>
struct self_get_turn_points
    <
        Reverse, multi_polygon_tag, MultiPolygon,
        TurnPolicy
    >
    : detail::self_get_turn_points::get_turns<Reverse, TurnPolicy>
{};


} // namespace dispatch
#endif // DOXYGEN_NO_DISPATCH


#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace self_get_turn_points
{

// Version where Reverse can be specified manually. TODO: 
// can most probably be merged with self_get_turn_points::get_turn
template
<
    bool Reverse,
    typename AssignPolicy,
    typename Geometry,
    typename IntersectionStrategy,
    typename RobustPolicy,
    typename Turns,
    typename InterruptPolicy
>
inline void self_turns(Geometry const& geometry,
                       IntersectionStrategy const& strategy,
                       RobustPolicy const& robust_policy,
                       Turns& turns,
                       InterruptPolicy& interrupt_policy,
                       std::size_t source_index = 0)
{
    concepts::check<Geometry const>();

    typedef detail::overlay::get_turn_info<detail::overlay::assign_null_policy> turn_policy;

    dispatch::self_get_turn_points
            <
                Reverse,
                typename tag<Geometry>::type,
                Geometry,
                turn_policy
            >::apply(geometry, strategy, robust_policy, turns, interrupt_policy, source_index);
}

}} // namespace detail::self_get_turn_points
#endif // DOXYGEN_NO_DETAIL

/*!
    \brief Calculate self intersections of a geometry
    \ingroup overlay
    \tparam Geometry geometry type
    \tparam Turns type of intersection container
                (e.g. vector of "intersection/turn point"'s)
    \param geometry geometry
    \param strategy strategy to be used
    \param robust_policy policy to handle robustness issues
    \param turns container which will contain intersection points
    \param interrupt_policy policy determining if process is stopped
        when intersection is found
 */
template
<
    typename AssignPolicy,
    typename Geometry,
    typename IntersectionStrategy,
    typename RobustPolicy,
    typename Turns,
    typename InterruptPolicy
>
inline void self_turns(Geometry const& geometry,
                       IntersectionStrategy const& strategy,
                       RobustPolicy const& robust_policy,
                       Turns& turns,
                       InterruptPolicy& interrupt_policy,
                       std::size_t source_index = 0)
{
    concepts::check<Geometry const>();

    static bool const reverse =  detail::overlay::do_reverse
        <
            geometry::point_order<Geometry>::value
        >::value;

    detail::self_get_turn_points::self_turns
            <
                reverse,
                AssignPolicy
            >(geometry, strategy, robust_policy, turns, interrupt_policy, source_index);
}



}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAY_SELF_TURN_POINTS_HPP
