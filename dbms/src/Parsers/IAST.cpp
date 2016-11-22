#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>
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


size_t IAST::checkSize(size_t max_size) const
{
	size_t res = 1;
	for (const auto & child : children)
		res += child->checkSize(max_size);

	if (res > max_size)
		throw Exception("AST is too big. Maximum: " + toString(max_size), ErrorCodes::TOO_BIG_AST);

	return res;
}


String IAST::getTreeID() const
{
	std::stringstream s;
	s << getID();

	if (!children.empty())
	{
		s << "(";
		for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
		{
			if (it != children.begin())
				s << ", ";
			s << (*it)->getTreeID();
		}
		s << ")";
	}

	return s.str();
}


size_t IAST::checkDepthImpl(size_t max_depth, size_t level) const
{
	size_t res = level + 1;
	for (const auto & child : children)
	{
		if (level >= max_depth)
			throw Exception("AST is too deep. Maximum: " + toString(max_depth), ErrorCodes::TOO_DEEP_AST);
		res = std::max(res, child->checkDepthImpl(max_depth, level + 1));
	}

	return res;
}

}
