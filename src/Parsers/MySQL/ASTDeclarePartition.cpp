#include <Parsers/MySQL/ASTDeclarePartition.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Parsers/MySQL/ASTDeclareSubPartition.h>

namespace DB
{

namespace MySQLParser
{

ASTPtr ASTDeclarePartition::clone() const
{
    auto res = std::make_shared<ASTDeclarePartition>(*this);
    res->children.clear();

    if (options)
    {
        res->options = options->clone();
        res->children.emplace_back(res->options);
    }

    if (less_than)
    {
        res->less_than = less_than->clone();
        res->children.emplace_back(res->less_than);
    }

    if (in_expression)
    {
        res->in_expression = in_expression->clone();
        res->children.emplace_back(res->in_expression);
    }

    if (subpartitions)
    {
        res->subpartitions = subpartitions->clone();
        res->children.emplace_back(res->subpartitions);
    }

    return res;
}

bool ParserDeclarePartition::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"PARTITION"}.ignore(pos, expected))
        return false;

    ASTPtr options;
    ASTPtr less_than;
    ASTPtr in_expression;
    ASTPtr partition_name;

    ParserExpression p_expression;
    ParserIdentifier p_identifier;

    if (!p_identifier.parse(pos, partition_name, expected))
        return false;

    ParserKeyword p_values("VALUES");
    if (p_values.ignore(pos, expected))
    {
        if (ParserKeyword{"IN"}.ignore(pos, expected))
        {
            if (!p_expression.parse(pos, in_expression, expected))
                return false;
        }
        else if (ParserKeyword{"LESS THAN"}.ignore(pos, expected))
        {
            if (!p_expression.parse(pos, less_than, expected))
                return false;
        }
    }

    ParserDeclareOptions options_p{
        {
            OptionDescribe("ENGINE", "engine", std::make_shared<ParserIdentifier>()),
            OptionDescribe("STORAGE ENGINE", "engine", std::make_shared<ParserIdentifier>()),
            OptionDescribe("COMMENT", "comment", std::make_shared<ParserStringLiteral>()),
            OptionDescribe("DATA DIRECTORY", "data_directory", std::make_shared<ParserStringLiteral>()),
            OptionDescribe("INDEX DIRECTORY", "index_directory", std::make_shared<ParserStringLiteral>()),
            OptionDescribe("MAX_ROWS", "max_rows", std::make_shared<ParserLiteral>()),
            OptionDescribe("MIN_ROWS", "min_rows", std::make_shared<ParserLiteral>()),
            OptionDescribe("TABLESPACE", "tablespace", std::make_shared<ParserIdentifier>()),
        }
    };

    /// Optional options
    options_p.parse(pos, options, expected);

    ASTPtr subpartitions;
    if (ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
    {
        if (!DB::ParserList(std::make_unique<ParserDeclareSubPartition>(), std::make_unique<ParserToken>(TokenType::Comma))
                 .parse(pos, subpartitions, expected))
            return false;

        if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            return false;
    }

    auto partition_declare = std::make_shared<ASTDeclarePartition>();
    partition_declare->options = options;
    partition_declare->less_than = less_than;
    partition_declare->in_expression = in_expression;
    partition_declare->subpartitions = subpartitions;
    partition_declare->partition_name = partition_name->as<ASTIdentifier>()->name();

    if (options)
    {
        partition_declare->options = options;
        partition_declare->children.emplace_back(partition_declare->options);
    }

    if (partition_declare->less_than)
        partition_declare->children.emplace_back(partition_declare->less_than);

    if (partition_declare->in_expression)
        partition_declare->children.emplace_back(partition_declare->in_expression);

    if (partition_declare->subpartitions)
        partition_declare->children.emplace_back(partition_declare->subpartitions);

    node = partition_declare;
    return true;
}
}

}
