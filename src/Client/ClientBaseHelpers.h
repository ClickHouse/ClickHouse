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
struct Settings;

/// A copy of `settings` containing only the knobs that select the network compression codec
/// (`network_compression_method`, `network_zstd_compression_level` and the codec-validation flags),
/// and only those of them that were actually changed.
///
/// Helper queries issued by the client itself (autocomplete, documentation lookup, metadata for the AI
/// mode) must honor the user's choice of the network codec, but they must not inherit the rest of the
/// session settings: `Connection::sendQuery` serializes every changed setting into the query packet, so
/// passing the whole session would make such a query run under, for example, a non-default `dialect`.
///
/// `dialect = 'clickhouse'` is additionally forced explicitly: dropping the session `dialect` is not
/// enough, because the server takes the parser from the effective `dialect` of the authenticated user,
/// which a settings profile may itself default to Kusto or PRQL.
///
/// `compatibility` is forwarded and the values it derived are kept but marked unchanged: they still select
/// the client-side network codec, while the server re-derives them from `compatibility` itself — serializing
/// them explicitly would break under a profile that pins them as read-only. This mirrors what
/// `ClientBase::settingsWithoutCompatibilityDerived` does for ordinary queries.
Settings networkCompressionSettings(const Settings & settings);

/// Should we celebrate a bit?
bool isNewYearMode();

bool isChineseNewYearMode(const String & local_tz);

std::string getChineseZodiac();

bool isCloudEndpoint(const std::string & host);

#if USE_REPLXX
/// When `lexer_fallback` is set, input that cannot be parsed as complete queries (e.g. the query
/// fragments found in the documentation) is highlighted token-by-token using the lexer instead of being
/// left unhighlighted. The line editor keeps the default (parser-only) behaviour.
void highlight(const String & query, std::vector<replxx::Replxx::Color> & colors, const Context & context, int cursor_position, bool rainbow_parentheses, bool lexer_fallback = false);
String highlighted(const String & query, const Context & context, bool rainbow_parentheses, bool lexer_fallback = false);
#endif

String formatQuery(String query);

void skipSpacesAndComments(const char*& pos, const char* end, std::function<void(std::string_view)> comment_callback);

}
