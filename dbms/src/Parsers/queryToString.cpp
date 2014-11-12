#include <DB/Parsers/queryToString.h>
#include <DB/Parsers/formatAST.h>
#include <sstream>

namespace DB
{
	String queryToString(const ASTPtr & query)
	{
		std::ostringstream out;
		formatAST(*query, out, 0, false, true);

		return out.str();
	}
}
