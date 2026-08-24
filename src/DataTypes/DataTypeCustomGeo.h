#pragma once

#include <DataTypes/DataTypeCustom.h>

namespace DB
{

class DataTypePointName : public DataTypeCustomFixedName
{
public:
    DataTypePointName() : DataTypeCustomFixedName("Point") {}
};

class DataTypeLineStringName : public DataTypeCustomFixedName
{
public:
    DataTypeLineStringName() : DataTypeCustomFixedName("LineString") {}
};

class DataTypeMultiLineStringName : public DataTypeCustomFixedName
{
public:
    DataTypeMultiLineStringName() : DataTypeCustomFixedName("MultiLineString") {}
};

class DataTypeRingName : public DataTypeCustomFixedName
{
public:
    DataTypeRingName() : DataTypeCustomFixedName("Ring") {}
};

class DataTypePolygonName : public DataTypeCustomFixedName
{
public:
    DataTypePolygonName() : DataTypeCustomFixedName("Polygon") {}
};

class DataTypeMultiPolygonName : public DataTypeCustomFixedName
{
public:
    DataTypeMultiPolygonName() : DataTypeCustomFixedName("MultiPolygon") {}
};

}
