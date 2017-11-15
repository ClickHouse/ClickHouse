// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2007-2015 Barend Gehrels, Amsterdam, the Netherlands.
// Copyright (c) 2008-2015 Bruno Lalande, Paris, France.
// Copyright (c) 2009-2015 Mateusz Loskot, London, UK.
// Copyright (c) 2014-2015 Samuel Debionne, Grenoble, France.

// This file was modified by Oracle on 2015, 2016.
// Modifications copyright (c) 2015-2016, Oracle and/or its affiliates.

// Contributed and/or modified by Vissarion Fysikopoulos, on behalf of Oracle
// Contributed and/or modified by Menelaos Karavelas, on behalf of Oracle

// Parts of Boost.Geometry are redesigned from Geodan's Geographic Library
// (geolib/GGL), copyright (c) 1995-2010 Geodan, Amsterdam, the Netherlands.

// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_EXPAND_INTERFACE_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_EXPAND_INTERFACE_HPP

#include <boost/variant/apply_visitor.hpp>
#include <boost/variant/static_visitor.hpp>
#include <boost/variant/variant_fwd.hpp>

#include <boost/geometry/geometries/concepts/check.hpp>

#include <boost/geometry/algorithms/dispatch/expand.hpp>

#include <boost/geometry/strategies/default_strategy.hpp>

#include <boost/geometry/strategies/envelope.hpp>
#include <boost/geometry/strategies/cartesian/envelope_segment.hpp>
#include <boost/geometry/strategies/spherical/envelope_segment.hpp>
#include <boost/geometry/strategies/geographic/envelope_segment.hpp>

namespace boost { namespace geometry
{

namespace resolve_strategy
{

template <typename Geometry>
struct expand
{
    template <typename Box, typename Strategy>
    static inline void apply(Box& box,
                             Geometry const& geometry,
                             Strategy const& strategy)
    {
        dispatch::expand<Box, Geometry>::apply(box, geometry, strategy);
    }

    template <typename Box>
    static inline void apply(Box& box,
                             Geometry const& geometry,
                             default_strategy)
    {
        typedef typename point_type<Geometry>::type point_type;
        typedef typename coordinate_type<point_type>::type coordinate_type;

        typedef typename strategy::envelope::services::default_strategy
                <
                typename cs_tag<point_type>::type,
                coordinate_type
                >::type strategy_type;

        dispatch::expand<Box, Geometry>::apply(box, geometry, strategy_type());
    }
};

} //namespace resolve_strategy


namespace resolve_variant
{
    
template <typename Geometry>
struct expand
{
    template <typename Box, typename Strategy>
    static inline void apply(Box& box,
                             Geometry const& geometry,
                             Strategy const& strategy)
    {
        concepts::check<Box>();
        concepts::check<Geometry const>();
        concepts::check_concepts_and_equal_dimensions<Box, Geometry const>();
        
        resolve_strategy::expand<Geometry>::apply(box, geometry, strategy);
    }
};

template <BOOST_VARIANT_ENUM_PARAMS(typename T)>
struct expand<boost::variant<BOOST_VARIANT_ENUM_PARAMS(T)> >
{
    template <typename Box, typename Strategy>
    struct visitor: boost::static_visitor<void>
    {
        Box& m_box;
        Strategy const& m_strategy;
        
        visitor(Box& box, Strategy const& strategy)
            : m_box(box)
            , m_strategy(strategy)
        {}
        
        template <typename Geometry>
        void operator()(Geometry const& geometry) const
        {
            return expand<Geometry>::apply(m_box, geometry, m_strategy);
        }
    };
    
    template <class Box, typename Strategy>
    static inline void
    apply(Box& box,
          boost::variant<BOOST_VARIANT_ENUM_PARAMS(T)> const& geometry,
          Strategy const& strategy)
    {
        return boost::apply_visitor(visitor<Box, Strategy>(box, strategy),
                                    geometry);
    }
};
    
} // namespace resolve_variant
    
    
/***
*!
\brief Expands a box using the extend (envelope) of another geometry (box, point)
\ingroup expand
\tparam Box type of the box
\tparam Geometry of second geometry, to be expanded with the box
\param box box to expand another geometry with, might be changed
\param geometry other geometry
\param strategy_less
\param strategy_greater
\note Strategy is currently ignored
 *
template
<
    typename Box, typename Geometry,
    typename StrategyLess, typename StrategyGreater
>
inline void expand(Box& box, Geometry const& geometry,
            StrategyLess const& strategy_less,
            StrategyGreater const& strategy_greater)
{
    concepts::check_concepts_and_equal_dimensions<Box, Geometry const>();

    dispatch::expand<Box, Geometry>::apply(box, geometry);
}
***/

/*!
\brief Expands (with strategy)
\ingroup expand
\tparam Box type of the box
\tparam Geometry \tparam_geometry
\tparam Strategy \tparam_strategy{expand}
\param box box to be expanded using another geometry, mutable
\param geometry \param_geometry geometry which envelope (bounding box)
\param strategy \param_strategy{expand}
will be added to the box

\qbk{distinguish,with strategy}
\qbk{[include reference/algorithms/expand.qbk]}
 */
template <typename Box, typename Geometry, typename Strategy>
inline void expand(Box& box, Geometry const& geometry, Strategy const& strategy)
{

    resolve_variant::expand<Geometry>::apply(box, geometry, strategy);
}

/*!
\brief Expands a box using the bounding box (envelope) of another geometry
(box, point)
\ingroup expand
\tparam Box type of the box
\tparam Geometry \tparam_geometry
\param box box to be expanded using another geometry, mutable
\param geometry \param_geometry geometry which envelope (bounding box) will be
added to the box

\qbk{[include reference/algorithms/expand.qbk]}
 */
template <typename Box, typename Geometry>
inline void expand(Box& box, Geometry const& geometry)
{
    resolve_variant::expand<Geometry>::apply(box, geometry, default_strategy());
}

}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_EXPAND_INTERFACE_HPP
