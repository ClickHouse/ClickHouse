#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserQuery : public IParserBase
{
private:
    const char * end;
    bool allow_settings_after_format_in_insert = false;
    bool implicit_select = false;

    bool allow_execute_as = true;
    bool allow_in_parallel_with = true;

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
