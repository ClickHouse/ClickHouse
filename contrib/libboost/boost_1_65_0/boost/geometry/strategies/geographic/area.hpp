// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2016-2017 Oracle and/or its affiliates.
// Contributed and/or modified by Vissarion Fisikopoulos, on behalf of Oracle
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_STRATEGIES_GEOGRAPHIC_AREA_HPP
#define BOOST_GEOMETRY_STRATEGIES_GEOGRAPHIC_AREA_HPP


#include <boost/geometry/core/srs.hpp>

#include <boost/geometry/formulas/area_formulas.hpp>
#include <boost/geometry/formulas/flattening.hpp>

#include <boost/geometry/strategies/geographic/parameters.hpp>

#include <boost/math/special_functions/atanh.hpp>


namespace boost { namespace geometry
{

namespace strategy { namespace area
{

/*!
\brief Geographic area calculation
\ingroup strategies
\details Geographic area calculation by trapezoidal rule plus integral
         approximation that gives the ellipsoidal correction
\tparam PointOfSegment \tparam_segment_point
\tparam FormulaPolicy Formula used to calculate azimuths
\tparam SeriesOrder The order of approximation of the geodesic integral
\tparam Spheroid The spheroid model
\tparam CalculationType \tparam_calculation
\author See
- Danielsen JS, The area under the geodesic. Surv Rev 30(232): 61–66, 1989
- Charles F.F Karney, Algorithms for geodesics, 2011 https://arxiv.org/pdf/1109.4448.pdf

\qbk{
[heading See also]
[link geometry.reference.algorithms.area.area_2_with_strategy area (with strategy)]
}
*/
template
<
    typename PointOfSegment,
    typename FormulaPolicy = strategy::andoyer,
    std::size_t SeriesOrder = strategy::default_order<FormulaPolicy>::value,
    typename Spheroid = srs::spheroid<double>,
    typename CalculationType = void
>
class geographic
{
    // Switch between two kinds of approximation(series in eps and n v.s.series in k ^ 2 and e'^2)
    static const bool ExpandEpsN = true;
    // LongSegment Enables special handling of long segments
    static const bool LongSegment = false;

    //Select default types in case they are not set

    typedef typename boost::mpl::if_c
    <
        boost::is_void<CalculationType>::type::value,
        typename select_most_precise
            <
                typename coordinate_type<PointOfSegment>::type,
                double
            >::type,
        CalculationType
    >::type CT;

protected :
    struct spheroid_constants
    {
        Spheroid m_spheroid;
        CT const m_a2;  // squared equatorial radius
        CT const m_e2;  // squared eccentricity
        CT const m_ep2; // squared second eccentricity
        CT const m_ep;  // second eccentricity
        CT const m_c2;  // squared authalic radius

        inline spheroid_constants(Spheroid const& spheroid)
            : m_spheroid(spheroid)
            , m_a2(math::sqr(get_radius<0>(spheroid)))
            , m_e2(formula::flattening<CT>(spheroid)
                 * (CT(2.0) - CT(formula::flattening<CT>(spheroid))))
            , m_ep2(m_e2 / (CT(1.0) - m_e2))
            , m_ep(math::sqrt(m_ep2))
            , m_c2(authalic_radius(spheroid, m_a2, m_e2))
        {}
    };

    static inline CT authalic_radius(Spheroid const& sph, CT const& a2, CT const& e2)
    {
        CT const c0 = 0;

        if (math::equals(e2, c0))
        {
            return a2;
        }

        CT const sqrt_e2 = math::sqrt(e2);
        CT const c2 = 2;

        return (a2 / c2) +
                  ((math::sqr(get_radius<2>(sph)) * boost::math::atanh(sqrt_e2))
                   / (c2 * sqrt_e2));
    }

    struct area_sums
    {
        CT m_excess_sum;
        CT m_correction_sum;

        // Keep track if encircles some pole
        std::size_t m_crosses_prime_meridian;

        inline area_sums()
            : m_excess_sum(0)
            , m_correction_sum(0)
            , m_crosses_prime_meridian(0)
        {}
        inline CT area(spheroid_constants spheroid_const) const
        {
            CT result;

            CT sum = spheroid_const.m_c2 * m_excess_sum
                   + spheroid_const.m_e2 * spheroid_const.m_a2 * m_correction_sum;

            // If encircles some pole
            if (m_crosses_prime_meridian % 2 == 1)
            {
                std::size_t times_crosses_prime_meridian
                        = 1 + (m_crosses_prime_meridian / 2);

                result = CT(2.0)
                         * geometry::math::pi<CT>()
                         * spheroid_const.m_c2
                         * CT(times_crosses_prime_meridian)
                         - geometry::math::abs(sum);

                if (geometry::math::sign<CT>(sum) == 1)
                {
                    result = - result;
                }

            }
            else
            {
                result = sum;
            }

            return result;
        }
    };

public :
    typedef CT return_type;
    typedef PointOfSegment segment_point_type;
    typedef area_sums state_type;

    explicit inline geographic(Spheroid const& spheroid = Spheroid())
        : m_spheroid_constants(spheroid)
    {}

    inline void apply(PointOfSegment const& p1,
                      PointOfSegment const& p2,
                      area_sums& state) const
    {

        if (! geometry::math::equals(get<0>(p1), get<0>(p2)))
        {

            typedef geometry::formula::area_formulas
                <
                    CT, SeriesOrder, ExpandEpsN
                > area_formulas;

            typename area_formulas::return_type_ellipsoidal result =
                     area_formulas::template ellipsoidal<FormulaPolicy::template inverse>
                                             (p1, p2, m_spheroid_constants);

            state.m_excess_sum += result.spherical_term;
            state.m_correction_sum += result.ellipsoidal_term;

            // Keep track whenever a segment crosses the prime meridian
            geometry::formula::area_formulas<CT>
                    ::crosses_prime_meridian(p1, p2, state);
        }
    }

    inline return_type result(area_sums const& state) const
    {
        return state.area(m_spheroid_constants);
    }

private:
    spheroid_constants m_spheroid_constants;

};

#ifndef DOXYGEN_NO_STRATEGY_SPECIALIZATIONS

namespace services
{


template <typename Point>
struct default_strategy<geographic_tag, Point>
{
    typedef strategy::area::geographic<Point> type;
};

#endif // DOXYGEN_NO_STRATEGY_SPECIALIZATIONS

}

}} // namespace strategy::area




}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_STRATEGIES_GEOGRAPHIC_AREA_HPP
