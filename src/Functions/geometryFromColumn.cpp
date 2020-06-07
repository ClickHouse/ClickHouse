#include <Functions/geometryFromColumn.h>

namespace DB {


namespace {

size_t getArrayDepth(const ColumnWithTypeAndName & col, size_t max_depth)
{
    size_t depth = 0;
    DataTypePtr data_type = col.type;

    while (isArray(data_type) && depth != max_depth + 1)
    {
        data_type = static_cast<const DataTypeArray &>(*data_type).getNestedType();
    }

    return max_depth;
}

}

GeometryFromColumnParser makeGeometryFromColumnParser(const ColumnWithTypeAndName & col)
{
    switch (getArrayDepth(col, 3)) {
        case 0: return Float64PointFromColumnParser(*col.column);
        case 1: return Float64RingFromColumnParser(*col.column);
        // case 2: return parsePolygon(col, i);
        // case 3: return parseMultyPoligon(col, i);
        default: throw Exception("Cannot parse geometry from column with type " + col.type->getName()
                + ", array depth is too big", ErrorCodes::ILLEGAL_COLUMN);
    }
}

}
