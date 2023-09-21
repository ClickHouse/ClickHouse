#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/// Optional conversion to interval alias. Support formats:
/// <nt>
///     <n> is a signed integer value like 1 +1 -1
///     <t> is a simple unit of time like s(seconds) or h(hours)
/// Example:
/// 1) "1s" parsed as "to_interval_second(1)"
///
/// The unit of time supports [Case Sensitive]:
/// 'nanoseconds' - 'ns'
/// 'microseconds' - 'us'
/// 'milliseconds' - 'ms'
/// 'seconds' - 's'
/// 'minutes' - 'm'
/// 'hours' - 'h'
/// 'days' - 'd'
/// 'weeks' - 'w'
/// 'months' - 'M'
/// 'quarter' - 'q'
/// 'year' - 'y' 
class ParserIntervalAliasExpression : public IParserBase
{
public:
    explicit ParserIntervalAliasExpression(ParserPtr && elem_parser_ = {}) : elem_parser(std::move(elem_parser_)) { }

private:
    ParserPtr elem_parser;

protected:
    const char * getName() const override { return "interval alias expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
