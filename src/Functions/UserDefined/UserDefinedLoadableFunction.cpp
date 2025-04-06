#include "UserDefinedLoadableFunction.h"

#include <Processors/Sources/ShellCommandSource.h>


namespace DB
{

UserDefinedLoadableFunction::UserDefinedLoadableFunction(
    const UserDefinedExecutableFunctionConfiguration & configuration_,
    std::shared_ptr<ShellCommandSourceCoordinator> coordinator_,
    const ExternalLoadableLifetime & lifetime_)
    : UserDefinedExecutableFunction(configuration_, std::move(coordinator_))
    , lifetime(lifetime_)
{
}

}
