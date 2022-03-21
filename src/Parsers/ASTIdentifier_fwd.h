#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>

#include <optional>
#include <vector>

namespace DB
{

class ASTIdentifier;
class ASTTableIdentifier;

/// ASTIdentifier Helpers: hide casts and semantic.

void setIdentifierSpecial(ASTPtr & ast);

String getIdentifierName(const IAST * ast);
std::optional<String> tryGetIdentifierName(const IAST * ast);
bool tryGetIdentifierNameInto(const IAST * ast, String & name);

inline String getIdentifierName(const ASTPtr & ast)
{
    return getIdentifierName(ast.get());
}
inline std::optional<String> tryGetIdentifierName(const ASTPtr & ast)
{
    return tryGetIdentifierName(ast.get());
}
inline bool tryGetIdentifierNameInto(const ASTPtr & ast, String & name)
{
    return tryGetIdentifierNameInto(ast.get(), name);
}

}
