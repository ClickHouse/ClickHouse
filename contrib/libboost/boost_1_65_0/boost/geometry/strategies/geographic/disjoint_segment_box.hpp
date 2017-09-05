// Boost.Geometry

// Copyright (c) 2017 Oracle and/or its affiliates.

// Contributed and/or modified by Vissarion Fysikopoulos, on behalf of Oracle
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_STRATEGIES_GEOGRAPHIC_DISJOINT_SEGMENT_BOX_HPP
#define BOOST_GEOMETRY_STRATEGIES_GEOGRAPHIC_DISJOINT_SEGMENT_BOX_HPP


#include <cstddef>
#include <utility>

#include <boost/numeric/conversion/cast.hpp>

#include <boost/geometry/util/math.hpp>
#include <boost/geometry/util/calculation_type.hpp>

#include <boost/geometry/core/access.hpp>
#include <boost/geometry/core/tags.hpp>
#include <boost/geometry/core/coordinate_dimension.hpp>
#include <boost/geometry/core/point_type.hpp>

#include <boost/geometry/algorithms/detail/assign_indexed_point.hpp>
#include <boost/geometry/algorithms/detail/disjoint/segment_box.hpp>

#include <boost/geometry/strategies/disjoint.hpp>
#include <boost/geometry/strategies/geographic/azimuth.hpp>
#include <boost/geometry/strategies/geographic/parameters.hpp>


namespace boost { namespace geometry { namespace strategy { namespace disjoint
{

// NOTE: This may be temporary place for this or corresponding strategy
// It seems to be more appropriate to implement the opposite of it
// e.g. intersection::segment_box because in disjoint() algorithm
// other strategies that are used are intersection and covered_by strategies.
template
<
    typename FormulaPolicy = strategy::andoyer,
    typename Spheroid = srs::spheroid<double>,
    typename CalculationType = void
>
struct segment_box_geographic
{
public:
    typedef Spheroid model_type;

    inline segment_box_geographic()
        : m_spheroid()
    {}

    explicit inline segment_box_geographic(Spheroid const& spheroid)
        : m_spheroid(spheroid)
    {}

    template <typename Segment, typename Box>
    struct point_in_geometry_strategy
        : services::default_strategy
            <
                typename point_type<Segment>::type,
                Box
            >
    {};

    template <typename Segment, typename Box>
    static inline typename point_in_geometry_strategy<Segment, Box>::type
        get_point_in_geometry_strategy()
    {
        typedef typename point_in_geometry_strategy<Segment, Box>::type strategy_type;

        return strategy_type();
    }

    template <typename Segment, typename Box>
    inline bool apply(Segment const& segment, Box const& box) const
    {
        geometry::strategy::azimuth::geographic
            <
                FormulaPolicy,
                Spheroid,
                CalculationType
            > azimuth_geographic(m_spheroid);

        return geometry::detail::disjoint::disjoint_segment_box_sphere_or_spheroid
                <geographic_tag>::apply(segment, box, azimuth_geographic);
    }

private:
    Spheroid m_spheroid;
};


#ifndef DOXYGEN_NO_STRATEGY_SPECIALIZATIONS


namespace services
{

template <typename Linear, typename Box, typename LinearTag>
struct default_strategy<Linear, Box, LinearTag, box_tag, 1, 2,
                        geographic_tag, geographic_tag>
{
    typedef segment_box_geographic<> type;
};

template <typename Box, typename Linear, typename LinearTag>
struct default_strategy<Box, Linear, box_tag, LinearTag, 2, 1,
                        geographic_tag, geographic_tag>
{
    typedef segment_box_geographic<> type;
};


} // namespace services


#endif // DOXYGEN_NO_STRATEGY_SPECIALIZATIONS


}}}} // namespace boost::geometry::strategy::disjoint


#endif // BOOST_GEOMETRY_STRATEGIES_GEOGRAPHIC_DISJOINT_SEGMENT_BOX_HPP
