#include <Functions/geometryFromColumn.h>

namespace DB {


namespace {

size_t getArrayDepth(DataTypePtr data_type, size_t max_depth)
{
    size_t depth = 0;
    while (isArray(data_type) && depth != max_depth + 1)
    {
        data_type = static_cast<const DataTypeArray &>(*data_type).getNestedType();
    }
    return max_depth;
}

}

GeometryFromColumnParser makeGeometryFromColumnParser(const ColumnWithTypeAndName & col)
{
    switch (getArrayDepth(col.type, 3)) {
        case 0: return Float64PointFromColumnParser(*col.column);
        case 1: return Float64RingFromColumnParser(*col.column);
        case 2: return Float64PolygonFromColumnParser(*col.column);
        case 3: return Float64MultiPolygonFromColumnParser(*col.column);
        default: throw Exception("Cannot parse geometry from column with type " + col.type->getName()
                + ", array depth is too big", ErrorCodes::ILLEGAL_COLUMN);
    }
}

}
