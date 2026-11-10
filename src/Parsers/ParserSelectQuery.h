#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


class ParserSelectQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SELECT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    /// If this flag is enabled, it will accept queries without SELECT, e.g. 1 + 2,
    /// (without SELECT, WITH is also not allowed; queries starting with FROM are also not allowed without SELECT)
    /// in this case, ClickHouse can be used as a calculator in the command line.
    bool implicit_select = false;

public:
    explicit ParserSelectQuery(bool implicit_select_ = false) : implicit_select(implicit_select_) {}
};

}
