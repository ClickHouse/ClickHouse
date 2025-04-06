#include "UserDefinedExecutableFunction.h"


namespace DB
{

UserDefinedExecutableFunction::UserDefinedExecutableFunction(
    const UserDefinedExecutableFunctionConfiguration & configuration_,
    std::shared_ptr<ShellCommandSourceCoordinator> coordinator_)
    : configuration(configuration_)
    , coordinator(std::move(coordinator_))
{
}

}
