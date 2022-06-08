#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserKQLStatement : public IParserBase
{
private:
    const char * end;
    bool allow_settings_after_format_in_insert;
    const char * getName() const override { return "KQL Statement"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    explicit ParserKQLStatement(const char * end_, bool allow_settings_after_format_in_insert_ = false)
        : end(end_)
        , allow_settings_after_format_in_insert(allow_settings_after_format_in_insert_)
    {}
};


class ParserKQLWithOutput : public IParserBase
{
protected:
    const char * end;
    bool allow_settings_after_format_in_insert;
    const char * getName() const override { return "KQL with output"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    explicit ParserKQLWithOutput(const char * end_, bool allow_settings_after_format_in_insert_ = false)
        : end(end_)
        , allow_settings_after_format_in_insert(allow_settings_after_format_in_insert_)
    {}
};

class ParserKQLWithUnionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "KQL query, possibly with UNION"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

