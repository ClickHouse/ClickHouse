#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/ParserQuery.h>
#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Common/StringUtils.h>
#include <DB/Common/typeid_cast.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int SYNTAX_ERROR;
}


/** From position in (possible multiline) query, get line number and column number in line.
  * Used in syntax error message.
  */
static std::pair<size_t, size_t> getLineAndCol(IParser::Pos begin, IParser::Pos pos)
{
	size_t line = 0;

	IParser::Pos nl;
	while (nullptr != (nl = reinterpret_cast<IParser::Pos>(memchr(begin, '\n', pos - begin))))
	{
		++line;
		begin = nl + 1;
	}

	/// Lines numbered from 1.
	return { line + 1, pos - begin + 1 };
}


static std::string getSyntaxErrorMessage(
	IParser::Pos begin,
	IParser::Pos end,
	IParser::Pos max_parsed_pos,
	Expected expected,
	bool hilite,
	const std::string & description)
{
	String message;

	{
		WriteBufferFromString out(message);

		out << "Syntax error";

		if (!description.empty())
			out << " (" << description << ")";

		if (max_parsed_pos == end || *max_parsed_pos == ';')
		{
			out << ": failed at end of query.\n";

			if (expected && *expected && *expected != '.')
				out << "Expected " << expected;
		}
		else
		{
			out << ": failed at position " << (max_parsed_pos - begin + 1);

			/// If query is multiline.
			IParser::Pos nl = reinterpret_cast<IParser::Pos>(memchr(begin, '\n', end - begin));
			if (nullptr != nl && nl + 1 != end)
			{
				size_t line = 0;
				size_t col = 0;
				std::tie(line, col) = getLineAndCol(begin, max_parsed_pos);

				out << " (line " << line << ", col " << col << ")";
			}

			/// Hilite place of syntax error.
			if (hilite)
			{
				out << ":\n\n";
				out.write(begin, max_parsed_pos - begin);

				size_t bytes_to_hilite = 1;
				while (max_parsed_pos + bytes_to_hilite < end
					&& static_cast<unsigned char>(max_parsed_pos[bytes_to_hilite]) >= 0x80	/// UTF-8
					&& static_cast<unsigned char>(max_parsed_pos[bytes_to_hilite]) <= 0xBF)
					++bytes_to_hilite;

				out << "\033[41;1m" << std::string(max_parsed_pos, bytes_to_hilite) << "\033[0m";		/// Bright red background.
				out.write(max_parsed_pos + bytes_to_hilite, end - max_parsed_pos - bytes_to_hilite);
				out << "\n\n";

				if (expected && *expected && *expected != '.')
					out << "Expected " << expected;
			}
			else
			{
				out << ": " << std::string(max_parsed_pos, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, end - max_parsed_pos));

				if (expected && *expected && *expected != '.')
					out << ", expected " << expected;
			}
		}
	}

	return message;
}


ASTPtr tryParseQuery(
	IParser & parser,
	IParser::Pos & pos,
	IParser::Pos end,
	std::string & out_error_message,
	bool hilite,
	const std::string & description,
	bool allow_multi_statements)
{
	if (pos == end || *pos == ';')
	{
		out_error_message = "Empty query";
		return nullptr;
	}

	Expected expected = "";
	IParser::Pos begin = pos;
	IParser::Pos max_parsed_pos = pos;

	ASTPtr res;
	bool parse_res = parser.parse(pos, end, res, max_parsed_pos, expected);

	/// Parsed query must end with end of data or semicolon.
	if (!parse_res || (pos != end && *pos != ';'))
	{
		out_error_message = getSyntaxErrorMessage(begin, end, max_parsed_pos, expected, hilite, description);
		return nullptr;
	}

	/// If multi-statements are not allowed, then after semicolon, there must be no non-space characters.
	if (!allow_multi_statements && pos < end && *pos == ';')
	{
		++pos;
		while (pos < end && isWhitespaceASCII(*pos))
			++pos;

		if (pos < end)
		{
			out_error_message = getSyntaxErrorMessage(begin, end, pos, nullptr, hilite,
				(description.empty() ? std::string() : std::string(". ")) + "Multi-statements are not allowed");
			return nullptr;
		}
	}

	return res;
}


ASTPtr parseQueryAndMovePosition(
	IParser & parser,
	IParser::Pos & pos,
	IParser::Pos end,
	const std::string & description,
	bool allow_multi_statements)
{
	std::string error_message;
	ASTPtr res = tryParseQuery(parser, pos, end, error_message, false, description, allow_multi_statements);

	if (res)
		return res;

	throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);
}


ASTPtr parseQuery(
	IParser & parser,
	IParser::Pos begin,
	IParser::Pos end,
	const std::string & description)
{
	auto pos = begin;
	return parseQueryAndMovePosition(parser, pos, end, description, false);
}

std::pair<const char *, bool> splitMultipartQuery(const std::string & queries, std::vector<std::string> & queries_list)
{
	ASTPtr ast;
	ParserQuery parser;

	const char * begin = queries.data(); /// begin of current query
	const char * pos = begin; /// parser moves pos from begin to the end of current query
	const char * end = begin + queries.size();

	queries_list.clear();

	while (pos < end)
	{
		begin = pos;

		ast = parseQueryAndMovePosition(parser, pos, end, "", true);
		if (!ast)
			break;

		ASTInsertQuery * insert = typeid_cast<ASTInsertQuery *>(ast.get());

		if (insert && insert->data)
		{
			/// Inserting data is broken on new line
			pos = insert->data;
			while (*pos && *pos != '\n')
				++pos;
			insert->end = pos;
		}

		queries_list.emplace_back(queries.substr(begin - queries.data(), pos - begin));

		while (isWhitespaceASCII(*pos) || *pos == ';')
			++pos;
	}

	return std::make_pair(begin, pos == end);
}

}
