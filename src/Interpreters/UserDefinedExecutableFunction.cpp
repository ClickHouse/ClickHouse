#include "UserDefinedExecutableFunction.h"

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <DataStreams/ShellCommandSource.h>
#include <DataStreams/formatBlock.h>


namespace DB
{

UserDefinedExecutableFunction::UserDefinedExecutableFunction(
    const Config & config_,
    const ExternalLoadableLifetime & lifetime_)
    : config(config_)
    , lifetime(lifetime_)
{
}

};
