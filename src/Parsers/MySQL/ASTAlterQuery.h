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
    };

    /// For ADD INDEX
    ASTDeclareIndex * add_index;

    /// For ADD COLUMN
    ASTDeclareColumn * add_column;

};

class ASTAlterCommandList : public IAST
{
public:
    std::vector<ASTAlterCommand *> commands;

    void add(const ASTPtr & command)
    {
        commands.push_back(command->as<ASTAlterCommand>());
        children.push_back(command);
    }

    String getID(char) const override { return "AlterCommandList"; }

    ASTPtr clone() const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

class ASTAlterQuery : public IAST
{
public:
    String database;
    String table;

    ASTAlterCommandList * command_list = nullptr;

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
