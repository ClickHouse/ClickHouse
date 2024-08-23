#pragma once

#include <DataTypes/DataTypeCustom.h>

namespace DB
{

class DataTypePointName : public DataTypeCustomFixedName
{
public:
    DataTypePointName() : DataTypeCustomFixedName("Point") {}
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
