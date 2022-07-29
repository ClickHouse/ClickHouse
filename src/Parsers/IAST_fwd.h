#pragma once

#include <memory>
#include <vector>
#include <list>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using ASTs = std::vector<ASTPtr>;
using ASTList = std::list<ASTPtr>;

}
