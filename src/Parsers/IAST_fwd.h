#pragma once

#include <memory>
#include <absl/container/inlined_vector.h>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using ASTs = std::vector<ASTPtr>;

}
