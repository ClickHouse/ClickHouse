#pragma once

#include <Functions/JSONPath/Generators/IVisitor.h>

namespace DB {

template <typename JSONParser>
class IGenerator;

template <typename JSONParser>
using IVisitorPtr = std::shared_ptr<IVisitor<JSONParser>>;

template <typename JSONParser>
using VisitorList = std::vector<IVisitorPtr<JSONParser>>;

} // namespace DB
