#pragma once

#include <Parsers/IParser.h>
#include <base/types.h>


namespace DB
{

/// True if pos is at a trailing query-output clause owned by ParserQueryWithOutput
/// (FORMAT / SETTINGS / INTO OUTFILE / PARALLEL WITH). The optional access-entity name in
/// SHOW [ROW|MASKING] POLICIES ... and SHOW CREATE ... is a bare identifier, so without this
/// guard it would swallow these keywords instead of leaving them for the outer parser.
bool atQueryOutputTail(IParser::Pos & pos, Expected & expected);

/// Like backQuoteIfNeed, but also backtick-quotes a name that is a single-token query-output
/// keyword (FORMAT, SETTINGS). Such a name is a valid identifier, so backQuoteIfNeed leaves it
/// unquoted, but emitting it unquoted after SHOW ... would be re-parsed as the output clause by
/// atQueryOutputTail, breaking the AST formatting round-trip.
String backQuoteAccessEntityNameIfNeed(const String & name);

}
