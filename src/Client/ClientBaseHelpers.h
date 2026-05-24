#pragma once

#include <string_view>
#include <vector>
#include <Client/AnsiColor.h>
#include <Core/Types.h>


namespace DB
{

class Context;

/// Should we celebrate a bit?
bool isNewYearMode();

bool isChineseNewYearMode(const String & local_tz);

std::string getChineseZodiac();

bool isCloudEndpoint(const std::string & host);

/// Fill `colors` (one entry per Unicode code point in `query`) with SQL
/// syntax highlighting. If `cursor_position >= 0`, the matching brace under
/// the cursor and identifiers matching the one under the cursor are
/// rendered with brighter colors / underline.
void highlight(const String & query, std::vector<AnsiColor::Color> & colors, const Context & context, int cursor_position, bool rainbow_parentheses);

/// Render `query` as an ANSI-escaped string. The display width is identical
/// to `query`. Used by the rustyline-bridge highlighter callback.
String highlightAnsi(const String & query, const Context & context, int cursor_position, bool rainbow_parentheses);

String formatQuery(String query);

void skipSpacesAndComments(const char*& pos, const char* end, std::function<void(std::string_view)> comment_callback);

}
