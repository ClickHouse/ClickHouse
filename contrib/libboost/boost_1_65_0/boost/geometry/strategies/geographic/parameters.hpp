// Boost.Geometry

// Copyright (c) 2017, Oracle and/or its affiliates.
// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_STRATEGIES_GEOGRAPHIC_PARAMETERS_HPP
#define BOOST_GEOMETRY_STRATEGIES_GEOGRAPHIC_PARAMETERS_HPP


#include <boost/geometry/formulas/andoyer_inverse.hpp>
#include <boost/geometry/formulas/thomas_inverse.hpp>
#include <boost/geometry/formulas/vincenty_inverse.hpp>

#include <boost/mpl/assert.hpp>
#include <boost/mpl/integral_c.hpp>


namespace boost { namespace geometry { namespace strategy
{

struct andoyer
{
    template
    <
        typename CT,
        bool EnableDistance,
        bool EnableAzimuth,
        bool EnableReverseAzimuth = false,
        bool EnableReducedLength = false,
        bool EnableGeodesicScale = false
    >
    struct inverse
        : formula::andoyer_inverse
            <
                CT, EnableDistance,
                EnableAzimuth, EnableReverseAzimuth,
                EnableReducedLength, EnableGeodesicScale
            >
    {};
};

struct thomas
{
    template
    <
        typename CT,
        bool EnableDistance,
        bool EnableAzimuth,
        bool EnableReverseAzimuth = false,
        bool EnableReducedLength = false,
        bool EnableGeodesicScale = false
    >
    struct inverse
        : formula::thomas_inverse
            <
                CT, EnableDistance,
                EnableAzimuth, EnableReverseAzimuth,
                EnableReducedLength, EnableGeodesicScale
            >
    {};
};

struct vincenty
{
    template
    <
        typename CT,
        bool EnableDistance,
        bool EnableAzimuth,
        bool EnableReverseAzimuth = false,
        bool EnableReducedLength = false,
        bool EnableGeodesicScale = false
    >
    struct inverse
        : formula::vincenty_inverse
            <
                CT, EnableDistance,
                EnableAzimuth, EnableReverseAzimuth,
                EnableReducedLength, EnableGeodesicScale
            >
    {};
};


template <typename FormulaPolicy>
struct default_order
{
    BOOST_MPL_ASSERT_MSG
    (
        false, NOT_IMPLEMENTED_FOR_THIS_TYPE
        , (types<FormulaPolicy>)
    );
};

template<>
struct default_order<andoyer>
    : boost::mpl::integral_c<unsigned int, 1>
{};

template<>
struct default_order<thomas>
    : boost::mpl::integral_c<unsigned int, 2>
{};

template<>
struct default_order<vincenty>
    : boost::mpl::integral_c<unsigned int, 4>
{};

}}} // namespace boost::geometry::strategy


#endif // BOOST_GEOMETRY_STRATEGIES_GEOGRAPHIC_PARAMETERS_HPP
