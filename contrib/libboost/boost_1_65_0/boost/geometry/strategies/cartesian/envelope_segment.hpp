// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2017 Oracle and/or its affiliates.
// Contributed and/or modified by Vissarion Fisikopoulos, on behalf of Oracle
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_STRATEGIES_CARTESIAN_ENVELOPE_SEGMENT_HPP
#define BOOST_GEOMETRY_STRATEGIES_CARTESIAN_ENVELOPE_SEGMENT_HPP


#include <boost/geometry/algorithms/detail/envelope/segment.hpp>
#include <boost/geometry/core/tags.hpp>
#include <boost/geometry/strategies/envelope.hpp>
#include <boost/geometry/util/select_calculation_type.hpp>


namespace boost { namespace geometry
{

namespace strategy { namespace envelope
{

template
<
    typename CalculationType = void
>
class cartesian_segment
{
public :

    template <typename Point1, typename Point2, typename Box>
    inline void
    apply(Point1 const& point1, Point2 const& point2, Box& box) const
    {
        geometry::detail::envelope::envelope_one_segment
                <
                    0,
                    dimension<Point1>::value
                >
                ::apply(point1,
                        point2,
                        box,
                        strategy::envelope::cartesian_segment<CalculationType>());
    }

};

#ifndef DOXYGEN_NO_STRATEGY_SPECIALIZATIONS

namespace services
{

template <typename CalculationType>
struct default_strategy<cartesian_tag, CalculationType>
{
    typedef strategy::envelope::cartesian_segment<CalculationType> type;
};

}

#endif // DOXYGEN_NO_STRATEGY_SPECIALIZATIONS


}} // namespace strategy::envelope

}} //namepsace boost::geometry

#endif // BOOST_GEOMETRY_STRATEGIES_CARTESIAN_ENVELOPE_SEGMENT_HPP
