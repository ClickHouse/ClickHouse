#pragma once

#include <unordered_map>

#include <Core/Types.h>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

using Aliases = std::unordered_map<String, ASTPtr>;

}
