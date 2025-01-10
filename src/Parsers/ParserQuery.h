#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserQuery : public IParserBase
{
private:
    const char * end;
    bool allow_settings_after_format_in_insert;
    bool implicit_select;

    const char * getName() const override { return "Query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserQuery(const char * end_, bool allow_settings_after_format_in_insert_ = false, bool implicit_select_ = false)
        : end(end_)
        , allow_settings_after_format_in_insert(allow_settings_after_format_in_insert_)
        , implicit_select(implicit_select_)
    {
    }
};

}
