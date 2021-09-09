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
    std::shared_ptr<scope_guard> function_deregister_,
    const ExternalLoadableLifetime & lifetime_)
    : config(config_)
    , function_deregister(std::move(function_deregister_))
    , lifetime(lifetime_)
{
}

};
