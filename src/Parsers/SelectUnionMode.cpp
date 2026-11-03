#include <Parsers/SelectUnionMode.h>


namespace DB
{

const char * toString(SelectUnionMode mode)
{
    switch (mode)
    {
        case SelectUnionMode::UNION_DEFAULT:
            return "UNION_DEFAULT";
        case SelectUnionMode::UNION_ALL:
            return "UNION_ALL";
        case SelectUnionMode::UNION_DISTINCT:
            return "UNION_DISTINCT";
        case SelectUnionMode::EXCEPT_DEFAULT:
            return "EXCEPT_DEFAULT";
        case SelectUnionMode::EXCEPT_ALL:
            return "EXCEPT_ALL";
        case SelectUnionMode::EXCEPT_DISTINCT:
            return "EXCEPT_DISTINCT";
        case SelectUnionMode::INTERSECT_DEFAULT:
            return "INTERSECT_DEFAULT";
        case SelectUnionMode::INTERSECT_ALL:
            return "INTERSECT_ALL";
        case SelectUnionMode::INTERSECT_DISTINCT:
            return "INTERSECT_DEFAULT";
    }
}

}
