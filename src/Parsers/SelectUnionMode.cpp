#include <Parsers/SelectUnionMode.h>
#include <stdexcept>


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

SelectUnionMode parseSelectUnionMode(const std::string & str)
{
    if (str == "UNION_DEFAULT")
        return SelectUnionMode::UNION_DEFAULT;
    if (str == "UNION_ALL")
        return SelectUnionMode::UNION_ALL;
    if (str == "UNION_DISTINCT")
        return SelectUnionMode::UNION_DISTINCT;
    if (str == "EXCEPT_DEFAULT")
        return SelectUnionMode::EXCEPT_DEFAULT;
    if (str == "EXCEPT_ALL")
        return SelectUnionMode::EXCEPT_ALL;
    if (str == "EXCEPT_DISTINCT")
        return SelectUnionMode::EXCEPT_DISTINCT;
    if (str == "INTERSECT_DEFAULT")
        return SelectUnionMode::INTERSECT_DEFAULT;
    if (str == "INTERSECT_ALL")
        return SelectUnionMode::INTERSECT_ALL;
    if (str == "INTERSECT_DISTINCT")
        return SelectUnionMode::INTERSECT_DISTINCT;
    throw std::runtime_error("Unknown SelectUnionMode: " + str);
}

}
