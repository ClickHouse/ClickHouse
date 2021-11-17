#include "UserDefinedExecutableFunction.h"

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <Processors/Sources/ShellCommandSource.h>
#include <Formats/formatBlock.h>


namespace DB
{

UserDefinedExecutableFunction::UserDefinedExecutableFunction(
    const UserDefinedExecutableFunctionConfiguration & configuration_,
    const ExternalLoadableLifetime & lifetime_,
    std::shared_ptr<ProcessPool> process_pool_)
    : configuration(configuration_)
    , lifetime(lifetime_)
    , process_pool(process_pool_)
{
    if (!process_pool && configuration.type == UserDefinedExecutableFunctionType::executable_pool)
        process_pool = std::make_shared<ProcessPool>(configuration.pool_size == 0 ? std::numeric_limits<int>::max() : configuration.pool_size);
}

};
