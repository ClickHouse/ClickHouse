#pragma once

#include <AggregateFunctions/AggregateFunctionGroupPolygonOperations.h>

namespace DB
{

template <typename Point>
struct GroupPolygonIntersectionPolicy
{
    static constexpr const char * name = "groupPolygonIntersection";

    static bool canSkipAdd(const MultiPolygon<Point> & accumulated) { return accumulated.empty(); }

    static void combine(const MultiPolygon<Point> & a, const MultiPolygon<Point> & b, MultiPolygon<Point> & result)
    {
        boost::geometry::intersection(a, b, result);
    }

    static bool tryShortCircuitMerge(MultiPolygon<Point> & lhs, const MultiPolygon<Point> & rhs)
    {
        if (lhs.empty())
            return true;
        if (rhs.empty())
        {
            lhs.clear();
            return true;
        }
        return false;
    }
};

template <typename Point>
using AggregateFunctionGroupPolygonIntersection = AggregateFunctionGroupPolygonOp<Point, GroupPolygonIntersectionPolicy<Point>>;

}
