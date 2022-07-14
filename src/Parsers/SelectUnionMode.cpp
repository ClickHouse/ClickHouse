#include <Parsers/SelectUnionMode.h>


namespace DB
{

const char * toString(SelectUnionMode mode)
{
    switch (mode)
    {
        case SelectUnionMode::ALL:
            return "ALL";
        case SelectUnionMode::DISTINCT:
            return "DISTINCT";
        case SelectUnionMode::EXCEPT:
            return "EXCEPT";
        case SelectUnionMode::INTERSECT:
            return "INTERSECT";
        case SelectUnionMode::Unspecified:
            return "Unspecified";
    }
}

}
