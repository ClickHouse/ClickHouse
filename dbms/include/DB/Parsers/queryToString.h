#pragma once

#include <DB/Parsers/IAST.h>

namespace DB
{
	String queryToString(const ASTPtr & query);
}
