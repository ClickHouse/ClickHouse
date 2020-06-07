#include <Functions/geometryFromColumn.h>

namespace DB {

Geometry geometryFromColumn(const ColumnWithTypeAndName &, size_t)
{
    return Point(0.0, 0.0);
}

}
