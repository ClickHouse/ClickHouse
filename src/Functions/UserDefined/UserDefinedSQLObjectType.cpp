#include "UserDefinedSQLObjectType.h"
#include <base/defines.h>

namespace DB
{

const char * toString(UserDefinedSQLObjectType type)
{
    switch (type)
    {
        case UserDefinedSQLObjectType::Function:
            return "FUNCTION";
    }
    UNREACHABLE();
}

}
