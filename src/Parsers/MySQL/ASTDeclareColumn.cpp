#include <Parsers/MySQL/ASTDeclareColumn.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Parsers/MySQL/ASTDeclareReference.h>
#include <Parsers/MySQL/ASTDeclareConstraint.h>

namespace DB
{

namespace MySQLParser
{

ASTPtr ASTDeclareColumn::clone() const
{
    auto res = std::make_shared<ASTDeclareColumn>(*this);
    res->children.clear();

    if (data_type)
    {
        res->data_type = data_type->clone();
        res->children.emplace_back(res->data_type);
    }

    if (column_options)
    {
        res->column_options = column_options->clone();
        res->children.emplace_back(res->column_options);
    }

    return res;
}

bool ParserDeclareColumn::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr column_name;
    ASTPtr column_data_type;
    ASTPtr column_options;
    bool is_national = false;

    ParserExpression p_expression;
    ParserIdentifier p_identifier;

    if (!p_identifier.parse(pos, column_name, expected))
        return false;

    if (ParserKeyword("NATIONAL").checkWithoutMoving(pos, expected))
        is_national = true;
    else if (ParserKeyword("DOUBLE PRECISION").checkWithoutMoving(pos, expected))
        ParserKeyword("DOUBLE").ignore(pos, expected); /// hack skip DOUBLE

    if (!p_expression.parse(pos, column_data_type, expected))
        return false;

    if (!parseColumnDeclareOptions(pos, column_options, expected))
            return false;

    if (is_national)
    {
        if (!column_options)
            column_options = std::make_shared<ASTDeclareOptions>();
        column_options->as<ASTDeclareOptions>()->changes.insert(
            std::make_pair("is_national", std::make_shared<ASTLiteral>(Field(UInt64(1)))));
    }

    auto declare_column = std::make_shared<ASTDeclareColumn>();
    declare_column->name = column_name->as<ASTIdentifier>()->name;
    declare_column->data_type = column_data_type;
    declare_column->column_options = column_options;

    if (declare_column->data_type)
        declare_column->children.emplace_back(declare_column->data_type);

    if (declare_column->column_options)
        declare_column->children.emplace_back(declare_column->column_options);

    node = declare_column;
    return true;
}
bool ParserDeclareColumn::parseColumnDeclareOptions(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserDeclareOption p_non_generate_options{
        {
            OptionDescribe("ZEROFILL", "zero_fill", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("UNSIGNED", "is_unsigned", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("NULL", "is_null", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("NOT NULL", "is_null", std::make_unique<ParserAlwaysFalse>()),
            OptionDescribe("DEFAULT", "default", std::make_unique<ParserExpression>()),
            OptionDescribe("AUTO_INCREMENT", "auto_increment", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("UNIQUE", "unique_key", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("UNIQUE KEY", "unique_key", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("KEY", "primary_key", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("PRIMARY KEY", "primary_key", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("COMMENT", "comment", std::make_unique<ParserStringLiteral>()),
            OptionDescribe("CHARACTER SET", "charset_name", std::make_unique<ParserCharsetName>()),
            OptionDescribe("COLLATE", "collate", std::make_unique<ParserCharsetName>()),
            OptionDescribe("COLUMN_FORMAT", "column_format", std::make_unique<ParserIdentifier>()),
            OptionDescribe("STORAGE", "storage", std::make_unique<ParserIdentifier>()),
            OptionDescribe("AS", "generated", std::make_unique<ParserExpression>()),
            OptionDescribe("GENERATED ALWAYS AS", "generated", std::make_unique<ParserExpression>()),
            OptionDescribe("STORED", "is_stored", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("VIRTUAL", "is_stored", std::make_unique<ParserAlwaysFalse>()),
            OptionDescribe("", "reference", std::make_unique<ParserDeclareReference>()),
            OptionDescribe("", "constraint", std::make_unique<ParserDeclareConstraint>()),
        }
    };

    return p_non_generate_options.parse(pos, node, expected);
}

}

}
