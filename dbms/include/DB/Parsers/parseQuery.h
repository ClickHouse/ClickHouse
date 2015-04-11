#pragma once

#include <DB/Parsers/IParser.h>


namespace DB
{

/// Распарсить запрос или записать сообщение об ошибке в out_error_message.
ASTPtr tryParseQuery(
	IParser & parser,
	IParser::Pos begin,
	IParser::Pos end,
	std::string & out_error_message,
	bool hilite,
	const std::string & description);


/// Распарсить запрос или кинуть исключение с сообщением об ошибке.
ASTPtr parseQuery(
	IParser & parser,
	IParser::Pos begin,
	IParser::Pos end,
	const std::string & description);

}
