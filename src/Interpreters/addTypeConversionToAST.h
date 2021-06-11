#pragma once

#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// It will produce an expression with CAST to get an AST with the required type.
ASTPtr addTypeConversionToAST(ASTPtr && ast, const String & type_name);

}
