#pragma once

#include <Parsers/Kusto/IKQLParserBase.h>

namespace DB
{

class ParserKQLStatement : public IKQLParserBase
{
private:
    const char * end;
    bool allow_settings_after_format_in_insert;
    const char * getName() const override { return "KQL Statement"; }
    bool parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected) override;

public:
    explicit ParserKQLStatement(const char * end_, bool allow_settings_after_format_in_insert_ = false)
        : end(end_), allow_settings_after_format_in_insert(allow_settings_after_format_in_insert_)
    {
    }
};

class ParserKQLWithOutput : public IKQLParserBase
{
protected:
    const char * end;
    bool allow_settings_after_format_in_insert;
    const char * getName() const override { return "KQL with output"; }
    bool parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected) override;

public:
    explicit ParserKQLWithOutput(const char * end_, bool allow_settings_after_format_in_insert_ = false)
        : end(end_), allow_settings_after_format_in_insert(allow_settings_after_format_in_insert_)
    {
    }
};

class ParserKQLWithUnionQuery : public IKQLParserBase
{
protected:
    const char * getName() const override { return "KQL query, possibly with UNION"; }
    bool parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected) override;
};

class ParserKQLTableFunction : public IKQLParserBase
{
protected:
    const char * getName() const override { return "KQL() function"; }
    bool parseImpl(KQLPos & pos, ASTPtr & node, KQLExpected & expected) override;
};

}
