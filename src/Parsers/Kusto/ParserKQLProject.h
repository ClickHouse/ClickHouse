#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLProject : public ParserKQLBase
{
public:
    void addColumn(String column) {columns.insert(column);}

protected:
    const char * getName() const override { return "KQL project"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    std::unordered_set <String> columns;
};

}
