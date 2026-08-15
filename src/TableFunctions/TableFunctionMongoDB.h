#pragma once

#include <base/types.h>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
class ASTFunction;

std::pair<String, ASTPtr> getKeyValueMongoDBArgument(const ASTFunction * ast_func);

}

