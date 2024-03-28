#pragma once

#include <memory>

namespace DB
{
struct FieldRef;

class IFunctionBase;
class IDataType;

using DataTypePtr = std::shared_ptr<const IDataType>;
using FunctionBasePtr = std::shared_ptr<const IFunctionBase>;

FieldRef applyFunction(const FunctionBasePtr & func, const DataTypePtr & current_type, const FieldRef & field);
}
