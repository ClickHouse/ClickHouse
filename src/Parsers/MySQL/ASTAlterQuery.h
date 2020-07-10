#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/MySQL/ASTDeclareIndex.h>
#include <Parsers/MySQL/ASTDeclareColumn.h>
#include <Parsers/MySQL/ASTDeclareTableOptions.h>

namespace DB
{

namespace MySQLParser
{

class ASTAlterQuery : public IAST
{
public:
    String database;
    String table;
    ASTPtr command_list;

    ASTPtr clone() const override;

    String getID(char delim) const override { return "AlterQuery" + (delim + database) + delim + table; }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

class ParserAlterQuery : public IParserBase
{
protected:
    const char * getName() const override { return "alter query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

}
