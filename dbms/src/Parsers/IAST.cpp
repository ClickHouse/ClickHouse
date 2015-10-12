#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/Parsers/IAST.h>


namespace DB
{

constexpr IAST::Attributes IAST::IsVisited;
constexpr IAST::Attributes IAST::IsPreprocessedForInJoinSubqueries;
constexpr IAST::Attributes IAST::IsIn;
constexpr IAST::Attributes IAST::IsNotIn;
constexpr IAST::Attributes IAST::IsJoin;
constexpr IAST::Attributes IAST::IsGlobal;

const char * IAST::hilite_keyword 		= "\033[1m";
const char * IAST::hilite_identifier 	= "\033[0;36m";
const char * IAST::hilite_function 		= "\033[0;33m";
const char * IAST::hilite_operator 		= "\033[1;33m";
const char * IAST::hilite_alias 		= "\033[0;32m";
const char * IAST::hilite_none 			= "\033[0m";


/// Квотировать идентификатор обратными кавычками, если это требуется.
String backQuoteIfNeed(const String & x)
{
	String res(x.size(), '\0');
	{
		WriteBufferFromString wb(res);
		writeProbablyBackQuotedString(x, wb);
	}
	return res;
}


void IAST::writeAlias(const String & name, std::ostream & s, bool hilite) const
{
	s << (hilite ? hilite_keyword : "") << " AS " << (hilite ? hilite_alias : "");

	WriteBufferFromOStream wb(s, 32);
	writeProbablyBackQuotedString(name, wb);
	wb.next();

	s << (hilite ? hilite_none : "");
}

}
