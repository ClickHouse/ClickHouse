// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2016-2017 Oracle and/or its affiliates.
// Contributed and/or modified by Vissarion Fisikopoulos, on behalf of Oracle
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_STRATEGIES_SPHERICAL_AZIMUTH_HPP
#define BOOST_GEOMETRY_STRATEGIES_SPHERICAL_AZIMUTH_HPP


#include <boost/geometry/strategies/azimuth.hpp>
#include <boost/geometry/formulas/spherical.hpp>

#include <boost/mpl/if.hpp>
#include <boost/type_traits/is_void.hpp>


namespace boost { namespace geometry
{

namespace strategy { namespace azimuth
{

template
<
    typename CalculationType = void
>
class spherical
{
public :

    inline spherical()
    {}

    template <typename T>
    static inline void apply(T const& lon1_rad, T const& lat1_rad,
                             T const& lon2_rad, T const& lat2_rad,
                             T& a1, T& a2)
    {
        typedef typename boost::mpl::if_
            <
                boost::is_void<CalculationType>, T, CalculationType
            >::type calc_t;

        geometry::formula::result_spherical<calc_t>
            result = geometry::formula::spherical_azimuth<calc_t, true>(
                        calc_t(lon1_rad), calc_t(lat1_rad),
                        calc_t(lon2_rad), calc_t(lat2_rad));

        a1 = result.azimuth;
        a2 = result.reverse_azimuth;
    }

    template <typename T>
    inline void apply(T const& lon1_rad, T const& lat1_rad,
                      T const& lon2_rad, T const& lat2_rad,
                      T& a1) const
    {
         typedef typename boost::mpl::if_
            <
                boost::is_void<CalculationType>, T, CalculationType
            >::type calc_t;

        geometry::formula::result_spherical<calc_t>
            result = geometry::formula::spherical_azimuth<calc_t, false>(
                        calc_t(lon1_rad), calc_t(lat1_rad),
                        calc_t(lon2_rad), calc_t(lat2_rad));

        a1 = result.azimuth;
    }

};

#ifndef DOXYGEN_NO_STRATEGY_SPECIALIZATIONS

namespace services
{

template <typename CalculationType>
struct default_strategy<spherical_equatorial_tag, CalculationType>
{
    typedef strategy::azimuth::spherical<CalculationType> type;
};

/*
template <typename CalculationType>
struct default_strategy<spherical_polar_tag, CalculationType>
{
    typedef strategy::azimuth::spherical<CalculationType> type;
};
*/
}

#endif // DOXYGEN_NO_STRATEGY_SPECIALIZATIONS

}} // namespace strategy::azimuth


}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_STRATEGIES_SPHERICAL_AZIMUTH_HPP
