#include "config_core.h"

#include <Functions/FunctionFactory.h>


namespace DB
{

void registerFunctions()
{
    auto & factory = FunctionFactory::instance();

    for (const auto & [_, reg] : FunctionRegisterMap::instance())
        reg(factory);
}

}
