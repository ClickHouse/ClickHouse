#pragma once

#include <Parsers/IAST.h>

namespace DB
{
    String queryToString(const ASTPtr & query, bool mask_password = false);
    String queryToString(const IAST & query, bool mask_password = false);
}
