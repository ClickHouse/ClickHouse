#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>
#include <Functions/geometryFromColumn.h>


namespace DB {

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace {

Exception failedToParse(const ColumnWithTypeAndName & col, std::string reason = "")
{
    return Exception("Cannot parse geometry from column with type " + col.type->getName()
            + (reason.empty() ? std::string() : ", " + reason), ErrorCodes::ILLEGAL_COLUMN);
}

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

PointFromColumnParser::PointFromColumnParser(const ColumnWithTypeAndName & col)
{
    const auto & tuple_columns = static_cast<const ColumnTuple &>(*col.column).getColumns();

    if (tuple_columns.size() != 2) {
        throw failedToParse(col, "tuple must have exactly 2 columns");
    }

    x = static_cast<const ColumnFloat64 &>(*tuple_columns[0]).getData().data();
    y = static_cast<const ColumnFloat64 &>(*tuple_columns[1]).getData().data();
}

Point PointFromColumnParser::createContainer() const
{
    return Point();
}

void PointFromColumnParser::get(Point & container, size_t i) const
{
    boost::geometry::set<0>(container, x[i]);
    boost::geometry::set<0>(container, y[i]);
}

GeometryFromColumnParser makeGeometryFromColumnParser(const ColumnWithTypeAndName & col)
{
    switch (getArrayDepth(col, 3)) {
        case 0: return PointFromColumnParser(col);
        // case 1: return parseRing(col, i);
        // case 2: return parsePolygon(col, i);
        // case 3: return parseMultyPoligon(col, i);
        default: throw failedToParse(col, "array depth is too big");
    }
}

}
