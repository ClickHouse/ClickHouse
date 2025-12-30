#pragma once

#include <memory>

namespace DB
{
class ICommand;

using CommandPtr = std::shared_ptr<ICommand>;
}
