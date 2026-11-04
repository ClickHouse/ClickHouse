#pragma once

#include <string_view>
#include <Core/Types.h>
#include "config.h"

#if USE_REPLXX
#   include <Client/ReplxxLineReader.h>
#endif


namespace DB
{

class Context;

/// Should we celebrate a bit?
bool isNewYearMode();

bool isChineseNewYearMode(const String & local_tz);

std::string getChineseZodiac();

bool isCloudEndpoint(const std::string & host);

#if USE_REPLXX
/// When `lexer_fallback` is set, input that cannot be parsed as complete queries (e.g. the query
/// fragments found in the documentation) is highlighted token-by-token using the lexer instead of being
/// left unhighlighted. The line editor keeps the default (parser-only) behaviour.
/// If `last_is_delimiter` is not null, it is set to whether the query ends with a delimiter (and left
/// intact if the query ends with whitespace).
void highlight(const String & query, std::vector<replxx::Replxx::Color> & colors, const Context & context, int cursor_position, bool rainbow_parentheses, bool * last_is_delimiter, bool lexer_fallback = false);
String highlighted(const String & query, const Context & context, bool rainbow_parentheses, bool lexer_fallback = false);
#endif

String formatQuery(String query);

void skipSpacesAndComments(const char*& pos, const char* end, std::function<void(std::string_view)> comment_callback);

}
