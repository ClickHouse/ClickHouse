#pragma once

#include <DB/Parsers/formatAST.h>

namespace DB
{
	inline std::string queryToString(const ASTPtr & query)
	{
		std::ostringstream s;
		formatAST(*query, s, 0, false, true);

		return s.str();
	}
}
