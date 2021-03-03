#pragma once

#include <Functions/geometryTypes.h>
#include <Common/Exception.h>

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

/// It must work only with CartesianPoint
class PointMercatorConverter
{
public:
    static void forward(CartesianPoint & point);
    static void backward(CartesianPoint & point);
};


class RingMercatorConverter
{
public:
    static void forward(CartesianRing & ring);
    static void backward(CartesianRing & ring);
};


class PolygonMercatorConverter
{
public:
    static void forward(CartesianPolygon & polygon);
    static void backward(CartesianPolygon & polygon);
};



class MultiPolygonMercatorConverter
{
public:
    static void forward(CartesianMultiPolygon & polygon);
    static void backward(CartesianMultiPolygon & polygon);
};


struct PType
{
    using Type = PType;
};


template <typename Geometry>
void mercatorForward(Geometry & geometry)
{
    if constexpr (std::is_same_v<Geometry, CartesianPoint>)
        return PointMercatorConverter::forward(geometry);
    else if constexpr (std::is_same_v<Geometry, CartesianRing>)
        return RingMercatorConverter::forward(geometry);
    else if constexpr (std::is_same_v<Geometry, CartesianPolygon>)
        return PolygonMercatorConverter::forward(geometry);
    else if constexpr (std::is_same_v<Geometry, CartesianMultiPolygon>)
        return MultiPolygonMercatorConverter::forward(geometry);
    else
        throw Exception("Unknown geometry type", ErrorCodes::LOGICAL_ERROR);
}


template <typename Geometry>
void mercatorBackward(Geometry & geometry)
{
    if constexpr (std::is_same_v<Geometry, CartesianPoint>)
        return PointMercatorConverter::backward(geometry);
    else if constexpr (std::is_same_v<Geometry, CartesianRing>)
        return RingMercatorConverter::backward(geometry);
    else if constexpr (std::is_same_v<Geometry, CartesianPolygon>)
        return PolygonMercatorConverter::backward(geometry);
    else if constexpr (std::is_same_v<Geometry, CartesianMultiPolygon>)
        return MultiPolygonMercatorConverter::backward(geometry);
    else
        throw Exception("Unknown geometry type", ErrorCodes::LOGICAL_ERROR);
}

}
