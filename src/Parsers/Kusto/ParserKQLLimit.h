#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

class ParserKQLLimit : public ParserKQLBase
{
public:
    void setTableName(String table_name_) {table_name = table_name_;}

protected:
    const char * getName() const override { return "KQL limit"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    String table_name;
};

}
