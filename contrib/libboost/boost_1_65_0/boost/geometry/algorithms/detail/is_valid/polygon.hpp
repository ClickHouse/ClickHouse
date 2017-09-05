// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2017 Adam Wulkiewicz, Lodz, Poland.

// Copyright (c) 2014-2017, Oracle and/or its affiliates.

// Contributed and/or modified by Menelaos Karavelas, on behalf of Oracle
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Licensed under the Boost Software License version 1.0.
// http://www.boost.org/users/license.html

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_IS_VALID_POLYGON_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_IS_VALID_POLYGON_HPP

#include <cstddef>
#ifdef BOOST_GEOMETRY_TEST_DEBUG
#include <iostream>
#endif // BOOST_GEOMETRY_TEST_DEBUG

#include <algorithm>
#include <deque>
#include <iterator>
#include <set>
#include <vector>

#include <boost/core/ignore_unused.hpp>
#include <boost/range.hpp>

#include <boost/geometry/core/assert.hpp>
#include <boost/geometry/core/exterior_ring.hpp>
#include <boost/geometry/core/interior_rings.hpp>
#include <boost/geometry/core/ring_type.hpp>
#include <boost/geometry/core/tags.hpp>

#include <boost/geometry/util/condition.hpp>
#include <boost/geometry/util/range.hpp>

#include <boost/geometry/geometries/box.hpp>

#include <boost/geometry/iterators/point_iterator.hpp>

#include <boost/geometry/algorithms/covered_by.hpp>
#include <boost/geometry/algorithms/disjoint.hpp>
#include <boost/geometry/algorithms/expand.hpp>
#include <boost/geometry/algorithms/num_interior_rings.hpp>
#include <boost/geometry/algorithms/validity_failure_type.hpp>
#include <boost/geometry/algorithms/detail/point_on_border.hpp>
#include <boost/geometry/algorithms/within.hpp>

#include <boost/geometry/algorithms/detail/check_iterator_range.hpp>
#include <boost/geometry/algorithms/detail/partition.hpp>

#include <boost/geometry/algorithms/detail/is_valid/complement_graph.hpp>
#include <boost/geometry/algorithms/detail/is_valid/has_valid_self_turns.hpp>
#include <boost/geometry/algorithms/detail/is_valid/is_acceptable_turn.hpp>
#include <boost/geometry/algorithms/detail/is_valid/ring.hpp>

#include <boost/geometry/algorithms/detail/is_valid/debug_print_turns.hpp>
#include <boost/geometry/algorithms/detail/is_valid/debug_validity_phase.hpp>
#include <boost/geometry/algorithms/detail/is_valid/debug_complement_graph.hpp>

#include <boost/geometry/algorithms/dispatch/is_valid.hpp>


namespace boost { namespace geometry
{


#ifndef DOXYGEN_NO_DETAIL
namespace detail { namespace is_valid
{


template <typename Polygon, bool CheckRingValidityOnly = false>
class is_valid_polygon
{
protected:
    typedef debug_validity_phase<Polygon> debug_phase;

    template <typename VisitPolicy, typename Strategy>
    struct per_ring
    {
        per_ring(VisitPolicy& policy, Strategy const& strategy)
            : m_policy(policy)
            , m_strategy(strategy)
        {}

        template <typename Ring>
        inline bool apply(Ring const& ring) const
        {
            return detail::is_valid::is_valid_ring
                <
                    Ring, false, true
                >::apply(ring, m_policy, m_strategy);
        }

        VisitPolicy& m_policy;
        Strategy const& m_strategy;
    };

    template <typename InteriorRings, typename VisitPolicy, typename Strategy>
    static bool has_valid_interior_rings(InteriorRings const& interior_rings,
                                         VisitPolicy& visitor,
                                         Strategy const& strategy)
    {
        return
            detail::check_iterator_range
                <
                    per_ring<VisitPolicy, Strategy>,
                    true // allow for empty interior ring range
                >::apply(boost::begin(interior_rings),
                         boost::end(interior_rings),
                         per_ring<VisitPolicy, Strategy>(visitor, strategy));
    }

    struct has_valid_rings
    {
        template <typename VisitPolicy, typename Strategy>
        static inline bool apply(Polygon const& polygon,
                                 VisitPolicy& visitor,
                                 Strategy const& strategy)
        {
            typedef typename ring_type<Polygon>::type ring_type;

            // check validity of exterior ring
            debug_phase::apply(1);

            if (! detail::is_valid::is_valid_ring
                     <
                         ring_type,
                         false // do not check self intersections
                     >::apply(exterior_ring(polygon), visitor, strategy))
            {
                return false;
            }

            // check validity of interior rings
            debug_phase::apply(2);

            return has_valid_interior_rings(geometry::interior_rings(polygon),
                                            visitor,
                                            strategy);
        }
    };


    // Iterator value_type is a Ring or Polygon
    template <typename Iterator, typename Box>
    struct partition_item
    {
        explicit partition_item(Iterator it)
            : m_it(it)
            , m_is_initialized(false)
        {}

        Iterator get() const
        {
            return m_it;
        }

        template <typename EnvelopeStrategy>
        Box const& get_envelope(EnvelopeStrategy const& strategy) const
        {
            if (! m_is_initialized)
            {
                m_box = geometry::return_envelope<Box>(*m_it, strategy);
                m_is_initialized = true;
            }
            return m_box;
        }

    private:
        Iterator m_it;
        mutable Box m_box;
        mutable bool m_is_initialized;
    };

    // structs for partition -- start
    template <typename EnvelopeStrategy>
    struct expand_box
    {
        explicit expand_box(EnvelopeStrategy const& strategy) : m_strategy(strategy) {}

        template <typename Box, typename Iterator>
        inline void apply(Box& total, partition_item<Iterator, Box> const& item) const
        {
            geometry::expand(total, item.get_envelope(m_strategy));
        }

        EnvelopeStrategy const& m_strategy;
    };

    template <typename EnvelopeStrategy>
    struct overlaps_box
    {
        explicit overlaps_box(EnvelopeStrategy const& strategy) : m_strategy(strategy) {}

        template <typename Box, typename Iterator>
        inline bool apply(Box const& box, partition_item<Iterator, Box> const& item) const
        {
            return ! geometry::disjoint(item.get_envelope(m_strategy), box);
        }

        EnvelopeStrategy const& m_strategy;
    };


    template <typename WithinStrategy>
    struct item_visitor_type
    {
        bool items_overlap;
        WithinStrategy const& m_strategy;

        explicit item_visitor_type(WithinStrategy const& strategy)
            : items_overlap(false)
            , m_strategy(strategy)
        {}

        template <typename Item>
        inline bool is_within(Item const& first, Item const& second)
        {
            typename point_type<Polygon>::type point;
            typedef detail::point_on_border::point_on_range<true> pob;

            // TODO: this should check for a point on the interior, instead
            // of on border. Or it should check using the overlap function.

            return pob::apply(point, points_begin(first), points_end(first))
                    && geometry::within(point, second, m_strategy);
        }

        template <typename Iterator, typename Box>
        inline bool apply(partition_item<Iterator, Box> const& item1,
                          partition_item<Iterator, Box> const& item2)
        {
            if (! items_overlap
                && (is_within(*item1.get(), *item2.get())
                  || is_within(*item2.get(), *item1.get())))
            {
                items_overlap = true;
                return false; // interrupt
            }
            return true;
        }
    };
    // structs for partition -- end


    template
    <
        typename RingIterator,
        typename ExteriorRing,
        typename TurnIterator,
        typename VisitPolicy,
        typename Strategy
    >
    static inline bool are_holes_inside(RingIterator rings_first,
                                        RingIterator rings_beyond,
                                        ExteriorRing const& exterior_ring,
                                        TurnIterator turns_first,
                                        TurnIterator turns_beyond,
                                        VisitPolicy& visitor,
                                        Strategy const& strategy)
    {
        boost::ignore_unused(visitor);

        // collect the interior ring indices that have turns with the
        // exterior ring
        std::set<signed_size_type> ring_indices;
        for (TurnIterator tit = turns_first; tit != turns_beyond; ++tit)
        {
            if (tit->operations[0].seg_id.ring_index == -1)
            {
                BOOST_GEOMETRY_ASSERT(tit->operations[1].seg_id.ring_index != -1);
                ring_indices.insert(tit->operations[1].seg_id.ring_index);
            }
            else if (tit->operations[1].seg_id.ring_index == -1)
            {
                BOOST_GEOMETRY_ASSERT(tit->operations[0].seg_id.ring_index != -1);
                ring_indices.insert(tit->operations[0].seg_id.ring_index);
            }
        }

        // prepare strategy
        typedef typename std::iterator_traits<RingIterator>::value_type inter_ring_type;
        typename Strategy::template point_in_geometry_strategy
            <
                inter_ring_type, ExteriorRing
            >::type const in_exterior_strategy
            = strategy.template get_point_in_geometry_strategy<inter_ring_type, ExteriorRing>();

        signed_size_type ring_index = 0;
        for (RingIterator it = rings_first; it != rings_beyond;
             ++it, ++ring_index)
        {
            // do not examine interior rings that have turns with the
            // exterior ring
            if (ring_indices.find(ring_index) == ring_indices.end()
                && ! geometry::covered_by(range::front(*it), exterior_ring, in_exterior_strategy))
            {
                return visitor.template apply<failure_interior_rings_outside>();
            }
        }

        // collect all rings (exterior and/or interior) that have turns
        for (TurnIterator tit = turns_first; tit != turns_beyond; ++tit)
        {
            ring_indices.insert(tit->operations[0].seg_id.ring_index);
            ring_indices.insert(tit->operations[1].seg_id.ring_index);
        }

        typedef geometry::model::box<typename point_type<Polygon>::type> box_type;
        typedef partition_item<RingIterator, box_type> item_type;

        // put iterators for interior rings without turns in a vector
        std::vector<item_type> ring_iterators;
        ring_index = 0;
        for (RingIterator it = rings_first; it != rings_beyond;
             ++it, ++ring_index)
        {
            if (ring_indices.find(ring_index) == ring_indices.end())
            {
                ring_iterators.push_back(item_type(it));
            }
        }

        // prepare strategies
        typedef typename Strategy::template point_in_geometry_strategy
            <
                inter_ring_type, inter_ring_type
            >::type in_interior_strategy_type;
        in_interior_strategy_type const in_interior_strategy
            = strategy.template get_point_in_geometry_strategy<inter_ring_type, inter_ring_type>();
        typedef typename Strategy::envelope_strategy_type envelope_strategy_type;
        envelope_strategy_type const envelope_strategy
            = strategy.get_envelope_strategy();

        // call partition to check if interior rings are disjoint from
        // each other
        item_visitor_type<in_interior_strategy_type> item_visitor(in_interior_strategy);

        geometry::partition
            <
                box_type
            >::apply(ring_iterators, item_visitor,
                     expand_box<envelope_strategy_type>(envelope_strategy),
                     overlaps_box<envelope_strategy_type>(envelope_strategy));

        if (item_visitor.items_overlap)
        {
            return visitor.template apply<failure_nested_interior_rings>();
        }
        else
        {
            return visitor.template apply<no_failure>();
        }
    }

    template
    <
        typename InteriorRings,
        typename ExteriorRing,
        typename TurnIterator,
        typename VisitPolicy,
        typename Strategy
    >
    static inline bool are_holes_inside(InteriorRings const& interior_rings,
                                        ExteriorRing const& exterior_ring,
                                        TurnIterator first,
                                        TurnIterator beyond,
                                        VisitPolicy& visitor,
                                        Strategy const& strategy)
    {
        return are_holes_inside(boost::begin(interior_rings),
                                boost::end(interior_rings),
                                exterior_ring,
                                first,
                                beyond,
                                visitor,
                                strategy);
    }

    struct has_holes_inside
    {    
        template <typename TurnIterator, typename VisitPolicy, typename Strategy>
        static inline bool apply(Polygon const& polygon,
                                 TurnIterator first,
                                 TurnIterator beyond,
                                 VisitPolicy& visitor,
                                 Strategy const& strategy)
        {
            return are_holes_inside(geometry::interior_rings(polygon),
                                    geometry::exterior_ring(polygon),
                                    first,
                                    beyond,
                                    visitor,
                                    strategy);
        }
    };




    struct has_connected_interior
    {
        template <typename TurnIterator, typename VisitPolicy, typename Strategy>
        static inline bool apply(Polygon const& polygon,
                                 TurnIterator first,
                                 TurnIterator beyond,
                                 VisitPolicy& visitor,
                                 Strategy const& )
        {
            boost::ignore_unused(visitor);

            typedef typename std::iterator_traits
                <
                    TurnIterator
                >::value_type turn_type;
            typedef complement_graph<typename turn_type::point_type> graph;

            graph g(geometry::num_interior_rings(polygon) + 1);
            for (TurnIterator tit = first; tit != beyond; ++tit)
            {
                typename graph::vertex_handle v1 = g.add_vertex
                    ( tit->operations[0].seg_id.ring_index + 1 );
                typename graph::vertex_handle v2 = g.add_vertex
                    ( tit->operations[1].seg_id.ring_index + 1 );
                typename graph::vertex_handle vip = g.add_vertex(tit->point);

                g.add_edge(v1, vip);
                g.add_edge(v2, vip);
            }

#ifdef BOOST_GEOMETRY_TEST_DEBUG
            debug_print_complement_graph(std::cout, g);
#endif // BOOST_GEOMETRY_TEST_DEBUG

            if (g.has_cycles())
            {
                return visitor.template apply<failure_disconnected_interior>();
            }
            else
            {
                return visitor.template apply<no_failure>();
            }
        }
    };

public:
    template <typename VisitPolicy, typename Strategy>
    static inline bool apply(Polygon const& polygon,
                             VisitPolicy& visitor,
                             Strategy const& strategy)
    {
        if (! has_valid_rings::apply(polygon, visitor, strategy))
        {
            return false;
        }

        if (BOOST_GEOMETRY_CONDITION(CheckRingValidityOnly))
        {
            return true;
        }

        // compute turns and check if all are acceptable
        debug_phase::apply(3);

        typedef has_valid_self_turns<Polygon> has_valid_turns;

        std::deque<typename has_valid_turns::turn_type> turns;
        bool has_invalid_turns
            = ! has_valid_turns::apply(polygon, turns, visitor, strategy);
        debug_print_turns(turns.begin(), turns.end());

        if (has_invalid_turns)
        {
            return false;
        }

        // check if all interior rings are inside the exterior ring
        debug_phase::apply(4);

        if (! has_holes_inside::apply(polygon,
                                      turns.begin(), turns.end(),
                                      visitor,
                                      strategy))
        {
            return false;
        }

        // check whether the interior of the polygon is a connected set
        debug_phase::apply(5);

        return has_connected_interior::apply(polygon,
                                             turns.begin(),
                                             turns.end(),
                                             visitor,
                                             strategy);
    }
};


}} // namespace detail::is_valid
#endif // DOXYGEN_NO_DETAIL



#ifndef DOXYGEN_NO_DISPATCH
namespace dispatch
{


// A Polygon is always a simple geometric object provided that it is valid.
//
// Reference (for validity of Polygons): OGC 06-103r4 (6.1.11.1)
template <typename Polygon, bool AllowEmptyMultiGeometries>
struct is_valid
    <
        Polygon, polygon_tag, AllowEmptyMultiGeometries
    > : detail::is_valid::is_valid_polygon<Polygon>
{};


} // namespace dispatch
#endif // DOXYGEN_NO_DISPATCH


}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_IS_VALID_POLYGON_HPP
