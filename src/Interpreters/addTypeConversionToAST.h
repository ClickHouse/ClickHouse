#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <common/types.h>


namespace DB
{

class NamesAndTypesList;

/// It will produce an expression with CAST to get an AST with the required type.
ASTPtr addTypeConversionToAST(ASTPtr && ast, const String & type_name);

// If same type, then ignore the wrapper of CAST function
ASTPtr addTypeConversionToAST(ASTPtr && ast, const String & type_name, const NamesAndTypesList & all_columns, ContextPtr context);

}
