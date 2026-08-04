#include <Parsers/MySQL/ASTDeclareSubPartition.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

namespace MySQLParser
{

bool ParserDeclareSubPartition::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{Keyword::SUBPARTITION}.ignore(pos, expected))
        return false;

    ASTPtr options;
    ASTPtr logical_name;
    ParserIdentifier p_identifier;

    if (!p_identifier.parse(pos, logical_name, expected))
        return false;

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

    auto subpartition_declare = std::make_shared<ASTDeclareSubPartition>();
    subpartition_declare->options = options;
    subpartition_declare->logical_name = logical_name->as<ASTIdentifier>()->name();

    if (options)
    {
        subpartition_declare->options = options;
        subpartition_declare->children.emplace_back(subpartition_declare->options);
    }

    node = subpartition_declare;
    return true;
}

ASTPtr ASTDeclareSubPartition::clone() const
{
    auto res = std::make_shared<ASTDeclareSubPartition>(*this);
    res->children.clear();

    if (options)
    {
        res->options = options->clone();
        res->children.emplace_back(res->options);
    }

    return res;
}
}

}
