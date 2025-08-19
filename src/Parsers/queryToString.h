#pragma once

#include <Parsers/IAST.h>

namespace DB
{
    String queryToString(const ASTPtr & query);
    String queryToString(const IAST & query);
}
