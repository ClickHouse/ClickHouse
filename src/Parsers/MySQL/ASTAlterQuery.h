#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/MySQL/ASTDeclareIndex.h>
#include <Parsers/MySQL/ASTDeclareColumn.h>
#include <Parsers/MySQL/ASTDeclareTableOptions.h>

namespace DB
{

namespace MySQLParser
{

class ASTAlterCommand : public IAST
{
public:
    enum Type
    {
        ADD_COLUMN,
        ADD_INDEX,
        DROP_CONSTRAINT,
        DROP_COLUMN,
        DROP_INDEX,
        DROP_PRIMARY_KEY,
        DROP_FOREIGN_KEY,
    };

    /// For ADD INDEX
    ASTDeclareIndex * add_index;

    /// For ADD COLUMN
    ASTDeclareColumn * add_column;

};

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

class ParserAlterCommand : public IParserBase
{
protected:
    const char * getName() const override { return "alter command"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    bool parseAddCommand(Pos & pos, ASTPtr & node, Expected & expected);

    bool parseDropCommand(Pos & pos, ASTPtr & node, Expected & expected);
};

class ParserAlterQuery : public IParserBase
{
protected:
    const char * getName() const override { return "alter query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

}
