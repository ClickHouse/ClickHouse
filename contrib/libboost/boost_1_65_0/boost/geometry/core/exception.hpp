// Boost.Geometry (aka GGL, Generic Geometry Library)

// Copyright (c) 2007-2015 Barend Gehrels, Amsterdam, the Netherlands.
// Copyright (c) 2008-2015 Bruno Lalande, Paris, France.
// Copyright (c) 2009-2015 Mateusz Loskot, London, UK.

// This file was modified by Oracle on 2015.
// Modifications copyright (c) 2015 Oracle and/or its affiliates.

// Contributed and/or modified by Adam Wulkiewicz, on behalf of Oracle

// Parts of Boost.Geometry are redesigned from Geodan's Geographic Library
// (geolib/GGL), copyright (c) 1995-2010 Geodan, Amsterdam, the Netherlands.

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_CORE_EXCEPTION_HPP
#define BOOST_GEOMETRY_CORE_EXCEPTION_HPP

#include <exception>

namespace boost { namespace geometry
{

/*!
\brief Base exception class for Boost.Geometry algorithms
\ingroup core
\details This class is never thrown. All exceptions thrown in Boost.Geometry
    are derived from exception, so it might be convenient to catch it.
*/
class exception : public std::exception
{
public:
    virtual char const* what() const throw()
    {
        return "Boost.Geometry exception";
    }
};

/*!
\brief Invalid Input Exception
\ingroup core
\details The invalid_input_exception is thrown if an invalid attribute
         is passed into a function, e.g. a DE-9IM mask string code containing
         invalid characters passed into a de9im::mask constructor.
 */
class invalid_input_exception : public geometry::exception
{
public:

    inline invalid_input_exception() {}

    virtual char const* what() const throw()
    {
        return "Boost.Geometry Invalid-Input exception";
    }
};

/*!
\brief Empty Input Exception
\ingroup core
\details The empty_input_exception is thrown if free functions, e.g. distance,
    are called with empty geometries, e.g. a linestring
    without points, a polygon without points, an empty multi-geometry.
\qbk{
[heading See also]
\* [link geometry.reference.algorithms.area the area function]
\* [link geometry.reference.algorithms.distance the distance function]
\* [link geometry.reference.algorithms.length the length function]
}
 */
class empty_input_exception : public geometry::invalid_input_exception
{
public:

    inline empty_input_exception() {}

    virtual char const* what() const throw()
    {
        return "Boost.Geometry Empty-Input exception";
    }
};


}} // namespace boost::geometry

#endif // BOOST_GEOMETRY_CORE_EXCEPTION_HPP
