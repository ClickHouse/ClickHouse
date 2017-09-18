// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2007-2015 Barend Gehrels, Amsterdam, the Netherlands.
// Copyright (c) 2008-2015 Bruno Lalande, Paris, France.
// Copyright (c) 2009-2015 Mateusz Loskot, London, UK.

// This file was modified by Oracle on 2014, 2015, 2017.
// Modifications copyright (c) 2014-2017 Oracle and/or its affiliates.

// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Parts of Boost.Geometry are redesigned from Geodan's Geographic Library
// (geolib/GGL), copyright (c) 1995-2010 Geodan, Amsterdam, the Netherlands.

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAPS_INTERFACE_HPP
#define BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAPS_INTERFACE_HPP


#include <cstddef>

#include <boost/geometry/algorithms/not_implemented.hpp>

#include <boost/geometry/geometries/concepts/check.hpp>

#include <boost/geometry/algorithms/detail/relate/relate_impl.hpp>

#include <boost/geometry/strategies/relate.hpp>


namespace boost { namespace geometry
{

#ifndef DOXYGEN_NO_DISPATCH
namespace dispatch
{


template
<
    typename Geometry1,
    typename Geometry2,
    typename Tag1 = typename tag<Geometry1>::type,
    typename Tag2 = typename tag<Geometry2>::type
>
struct overlaps
    : detail::relate::relate_impl
        <
            detail::de9im::static_mask_overlaps_type,
            Geometry1,
            Geometry2
        >
{};


} // namespace dispatch
#endif // DOXYGEN_NO_DISPATCH


/*!
\brief \brief_check2{overlap}
\ingroup overlaps
\tparam Geometry1 \tparam_geometry
\tparam Geometry2 \tparam_geometry
\tparam Strategy \tparam_strategy{Overlaps}
\param geometry1 \param_geometry
\param geometry2 \param_geometry
\param strategy \param_strategy{overlaps}
\return \return_check2{overlap}

\qbk{distinguish,with strategy}
\qbk{[include reference/algorithms/overlaps.qbk]}
*/
template <typename Geometry1, typename Geometry2, typename Strategy>
inline bool overlaps(Geometry1 const& geometry1,
                     Geometry2 const& geometry2,
                     Strategy const& strategy)
{
    concepts::check<Geometry1 const>();
    concepts::check<Geometry2 const>();

    return dispatch::overlaps
        <
            Geometry1,
            Geometry2
        >::apply(geometry1, geometry2, strategy);
}

/*!
\brief \brief_check2{overlap}
\ingroup overlaps
\tparam Geometry1 \tparam_geometry
\tparam Geometry2 \tparam_geometry
\param geometry1 \param_geometry
\param geometry2 \param_geometry
\return \return_check2{overlap}

\qbk{[include reference/algorithms/overlaps.qbk]}
*/
template <typename Geometry1, typename Geometry2>
inline bool overlaps(Geometry1 const& geometry1, Geometry2 const& geometry2)
{
    concepts::check<Geometry1 const>();
    concepts::check<Geometry2 const>();

    typedef typename strategy::relate::services::default_strategy
            <
                Geometry1,
                Geometry2
            >::type strategy_type;

    return dispatch::overlaps
        <
            Geometry1,
            Geometry2
        >::apply(geometry1, geometry2, strategy_type());
}

}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_ALGORITHMS_DETAIL_OVERLAPS_INTERFACE_HPP
