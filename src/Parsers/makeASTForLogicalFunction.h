#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB
{
/// Makes an AST calculating argument1 AND argument2 AND ... AND argumentN.
ASTPtr makeASTForLogicalAnd(ASTs && arguments);

/// Makes an AST calculating argument1 OR argument2 OR ... OR argumentN.
ASTPtr makeASTForLogicalOr(ASTs && arguments);

/// Tries to extract a literal bool from AST.
bool tryGetLiteralBool(const IAST * ast, bool & value);
}
