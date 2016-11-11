#pragma once

#include <DB/Parsers/IParser.h>


namespace DB
{

/// Parse query or set 'out_error_message'.
ASTPtr tryParseQuery(
	IParser & parser,
	IParser::Pos & pos,				/// Moved to end of parsed fragment.
	IParser::Pos end,
	std::string & out_error_message,
	bool hilite,
	const std::string & description,
	bool allow_multi_statements);	/// If false, check for non-space characters after semicolon and set error message if any.


/// Parse query or throw an exception with error message.
ASTPtr parseQueryAndMovePosition(
	IParser & parser,
	IParser::Pos & pos,				/// Moved to end of parsed fragment.
	IParser::Pos end,
	const std::string & description,
	bool allow_multi_statements);


ASTPtr parseQuery(
	IParser & parser,
	IParser::Pos begin,
	IParser::Pos end,
	const std::string & description);


/** Split queries separated by ; on to list of single queries
  * Returns pointer to the end of last sucessfuly parsed query (first), and true if all queries are sucessfuly parsed (second)
  * NOTE: INSERT's data should be placed in single line.
  */
std::pair<const char *, bool> splitMultipartQuery(const std::string & queries, std::vector<std::string> & queries_list);

}
