#pragma once

#include <Parsers/IParser.h>
#include <Common/IntervalKind.h>


namespace DB
{
/// Parses an interval kind.
bool parseIntervalKind(IParser::Pos & pos, Expected & expected, IParser::Ranges * ranges, IntervalKind & result);
}
