#pragma once

#include <memory>

namespace DB
{

class IGeneralWindowFunction;
using GeneralWindowFunctionPtr = std::shared_ptr<const IGeneralWindowFunction>;

}
