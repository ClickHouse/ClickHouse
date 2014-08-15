#pragma once

#include <DB/Parsers/formatAST.h>

namespace DB
{
	template <typename ASTType>
	inline std::string queryToString(const ASTPtr & query)
	{
		const auto & query_ast = typeid_cast<const ASTType &>(*query);

		std::ostringstream s;
		formatAST(query_ast, s, 0, false, true);

		return s.str();
	}
}
