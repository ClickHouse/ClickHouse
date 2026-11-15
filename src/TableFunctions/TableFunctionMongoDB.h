#pragma once

#include <base/types.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTFunction;

std::pair<String, ASTPtr> getKeyValueMongoDBArgument(const ASTFunction * ast_func);

}

