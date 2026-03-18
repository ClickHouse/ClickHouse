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
    std::shared_ptr<ShellCommandSourceCoordinator> coordinator_,
    const ExternalLoadableLifetime & lifetime_)
    : configuration(configuration_)
    , coordinator(std::move(coordinator_))
    , lifetime(lifetime_)
{
}

}
