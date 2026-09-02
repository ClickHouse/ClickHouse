#include <Common/register_objects.h>

namespace DB
{

FunctionRegisterMap & FunctionRegisterMap::instance()
{
    static FunctionRegisterMap map;
    return map;
}

}
