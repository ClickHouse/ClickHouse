#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

/// Parse queries supporting [INTO OUTFILE 'file_name'] [FORMAT format_name] [SETTINGS key1 = value1, key2 = value2, ...] suffix.
class ParserQueryWithOutput : public IParserBase
{
public:
    ParserQueryWithOutput(bool enable_explain_ = false)
        : enable_explain(enable_explain_)
    {}

protected:
    const char * getName() const override { return "Query with output"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool enable_explain;
};

}
