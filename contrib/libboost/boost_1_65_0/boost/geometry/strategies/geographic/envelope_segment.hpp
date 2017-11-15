// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2017 Oracle and/or its affiliates.
// Contributed and/or modified by Vissarion Fisikopoulos, on behalf of Oracle
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_STRATEGIES_GEOGRAPHIC_ENVELOPE_SEGMENT_HPP
#define BOOST_GEOMETRY_STRATEGIES_GEOGRAPHIC_ENVELOPE_SEGMENT_HPP


#include <boost/geometry/algorithms/detail/envelope/segment.hpp>
#include <boost/geometry/algorithms/detail/normalize.hpp>
#include <boost/geometry/core/srs.hpp>
#include <boost/geometry/strategies/envelope.hpp>
#include <boost/geometry/strategies/geographic/azimuth.hpp>
#include <boost/geometry/strategies/geographic/parameters.hpp>


namespace boost { namespace geometry
{

namespace strategy { namespace envelope
{

template
<
    typename FormulaPolicy = strategy::andoyer,
    typename Spheroid = geometry::srs::spheroid<double>,
    typename CalculationType = void
>
class geographic_segment
{
public:
    typedef Spheroid model_type;

    inline geographic_segment()
        : m_spheroid()
    {}

    explicit inline geographic_segment(Spheroid const& spheroid)
        : m_spheroid(spheroid)
    {}

    template <typename Point1, typename Point2, typename Box>
    inline void apply(Point1 const& point1, Point2 const& point2, Box& box) const
    {
        Point1 p1_normalized = detail::return_normalized<Point1>(point1);
        Point2 p2_normalized = detail::return_normalized<Point2>(point2);

        geometry::strategy::azimuth::geographic
            <
                FormulaPolicy,
                Spheroid,
                CalculationType
            > azimuth_geographic(m_spheroid);

        typedef typename coordinate_system<Point1>::type::units units_type;

        detail::envelope::envelope_segment_impl
            <
                geographic_tag
            >::template apply<units_type>(geometry::get<0>(p1_normalized),
                                          geometry::get<1>(p1_normalized),
                                          geometry::get<0>(p2_normalized),
                                          geometry::get<1>(p2_normalized),
                                          box,
                                          azimuth_geographic);

    }

private:
    Spheroid m_spheroid;
};

#ifndef DOXYGEN_NO_STRATEGY_SPECIALIZATIONS

namespace services
{

template <typename CalculationType>
struct default_strategy<geographic_tag, CalculationType>
{
    typedef strategy::envelope::geographic_segment
        <
            strategy::andoyer,
            srs::spheroid<double>,
            CalculationType
        > type;
};

}

#endif // DOXYGEN_NO_STRATEGY_SPECIALIZATIONS


}} // namespace strategy::envelope

}} //namepsace boost::geometry

#endif // BOOST_GEOMETRY_STRATEGIES_GEOGRAPHIC_ENVELOPE_SEGMENT_HPP
