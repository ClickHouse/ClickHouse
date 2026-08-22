#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{
    ASTPtr extractKeyExpressionList(const ASTPtr & node);
}
