#pragma once

#include <Parsers/IAST.h>
#include <memory>
#include <unordered_map>

namespace DB
{

struct ASTHalfHash
{
    UInt64 operator()(const IAST::Hash & ast_hash) const { return ast_hash.first; }
};

class Set;
using SetPtr = std::shared_ptr<Set>;

using PreparedSets = std::unordered_map<IAST::Hash, SetPtr, ASTHalfHash>;

}
