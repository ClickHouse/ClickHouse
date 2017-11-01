// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2017 Oracle and/or its affiliates.
// Contributed and/or modified by Vissarion Fisikopoulos, on behalf of Oracle
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_STRATEGIES_SPHERICAL_ENVELOPE_SEGMENT_HPP
#define BOOST_GEOMETRY_STRATEGIES_SPHERICAL_ENVELOPE_SEGMENT_HPP

#include <boost/geometry/algorithms/detail/envelope/segment.hpp>
#include <boost/geometry/algorithms/detail/normalize.hpp>
#include <boost/geometry/strategies/envelope.hpp>
#include <boost/geometry/strategies/spherical/azimuth.hpp>

namespace boost { namespace geometry
{

namespace strategy { namespace envelope
{

template
<
    typename CalculationType = void
>
class spherical_segment
{
public :

    inline spherical_segment()
    {}

    template <typename Point1, typename Point2, typename Box>
    inline void
    apply(Point1 const& point1, Point2 const& point2, Box& box) const
    {
        Point1 p1_normalized = detail::return_normalized<Point1>(point1);
        Point2 p2_normalized = detail::return_normalized<Point2>(point2);

        geometry::strategy::azimuth::spherical<CalculationType> azimuth_spherical;

        typedef typename coordinate_system<Point1>::type::units units_type;

        geometry::detail::envelope::envelope_segment_impl<spherical_equatorial_tag>
                ::template apply<units_type>(geometry::get<0>(p1_normalized),
                                             geometry::get<1>(p1_normalized),
                                             geometry::get<0>(p2_normalized),
                                             geometry::get<1>(p2_normalized),
                                             box,
                                             azimuth_spherical);

  }
};

#ifndef DOXYGEN_NO_STRATEGY_SPECIALIZATIONS

namespace services
{

template <typename CalculationType>
struct default_strategy<spherical_equatorial_tag, CalculationType>
{
    typedef strategy::envelope::spherical_segment<CalculationType> type;
};


template <typename CalculationType>
struct default_strategy<spherical_polar_tag, CalculationType>
{
    typedef strategy::envelope::spherical_segment<CalculationType> type;
};

}

#endif // DOXYGEN_NO_STRATEGY_SPECIALIZATIONS


}} // namespace strategy::envelope

}} //namepsace boost::geometry

#endif // BOOST_GEOMETRY_STRATEGIES_SPHERICAL_ENVELOPE_SEGMENT_HPP

