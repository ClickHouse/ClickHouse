#pragma once

#include <memory>
#include <vector>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using ASTs = std::vector<ASTPtr>;

}
