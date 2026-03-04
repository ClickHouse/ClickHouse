#pragma once

#include <AggregateFunctions/AggregateFunctionGroupPolygonOperations.h>

namespace DB
{

template <typename Point>
struct GroupPolygonUnionPolicy
{
    static constexpr const char * name = "groupPolygonUnion";

    static bool canSkipAdd(const MultiPolygon<Point> &) { return false; }

    static void combine(const MultiPolygon<Point> & a, const MultiPolygon<Point> & b, MultiPolygon<Point> & result)
    {
        boost::geometry::union_(a, b, result);
    }

    static bool tryShortCircuitMerge(MultiPolygon<Point> &, const MultiPolygon<Point> &) { return false; }
};

template <typename Point>
using AggregateFunctionGroupPolygonUnion = AggregateFunctionGroupPolygonOp<Point, GroupPolygonUnionPolicy<Point>>;

}
