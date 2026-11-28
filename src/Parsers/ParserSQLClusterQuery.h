#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

class ParserCreateSQLClusterQuery : public IParserBase
{
public:
    [[nodiscard]] std::map<String, Documentation> getDocumentation() const override;

protected:
    [[nodiscard]] const char * getName() const override { return "CREATE SQL CLUSTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserAlterSQLClusterQuery : public IParserBase
{
public:
    [[nodiscard]] std::map<String, Documentation> getDocumentation() const override;

protected:
    [[nodiscard]] const char * getName() const override { return "ALTER SQL CLUSTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserDropSQLClusterQuery : public IParserBase
{
protected:
    [[nodiscard]] const char * getName() const override { return "DROP SQL CLUSTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
