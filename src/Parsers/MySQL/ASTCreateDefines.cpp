#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/MySQL/ASTCreateDefines.h>
#include <Parsers/MySQL/ASTDeclareIndex.h>
#include <Parsers/MySQL/ASTDeclareColumn.h>
#include <Parsers/MySQL/ASTDeclareConstraint.h>

namespace DB
{

namespace MySQLParser
{

class ParserCreateDefine : public IParserBase
{
protected:

    const char * getName() const override { return "table property (column, index, constraint)"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        MySQLParser::ParserDeclareIndex p_declare_index;
        MySQLParser::ParserDeclareColumn p_declare_column;
        MySQLParser::ParserDeclareConstraint p_declare_constraint;

        if (likely(!p_declare_index.parse(pos, node, expected)))
        {
            if (likely(!p_declare_constraint.parse(pos, node, expected)))
            {
                if (!p_declare_column.parse(pos, node, expected))
                    return false;
            }
        }

        return true;
    }
};

ASTPtr ASTCreateDefines::clone() const
{
    auto res = std::make_shared<ASTCreateDefines>(*this);
    res->children.clear();

    if (columns)
        res->set(res->columns, columns->clone());

    if (indices)
        res->set(res->indices, indices->clone());

    if (constraints)
        res->set(res->constraints, constraints->clone());

    return res;
}

bool ParserCreateDefines::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr create_defines;
    ParserList create_defines_parser(std::make_unique<ParserCreateDefine>(), std::make_unique<ParserToken>(TokenType::Comma), false);

    if (!create_defines_parser.parse(pos, create_defines, expected))
        return false;

    ASTPtr columns = std::make_shared<ASTExpressionList>();
    ASTPtr indices = std::make_shared<ASTExpressionList>();
    ASTPtr constraints = std::make_shared<ASTExpressionList>();

    for (const auto & create_define : create_defines->children)
    {
        if (create_define->as<ASTDeclareColumn>())
            columns->children.push_back(create_define);
        else if (create_define->as<ASTDeclareIndex>())
            indices->children.push_back(create_define);
        else if (create_define->as<ASTDeclareConstraint>())
            constraints->children.push_back(create_define);
        else
            return false;
    }

    auto res = std::make_shared<ASTCreateDefines>();
    if (!columns->children.empty())
        res->set(res->columns, columns);
    if (!indices->children.empty())
        res->set(res->indices, indices);
    if (!constraints->children.empty())
        res->set(res->constraints, constraints);

    node = res;
    return true;
}

}

}

