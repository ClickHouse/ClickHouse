#include <Parsers/MySQL/ASTDeclareColumn.h>

#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/MySQL/ASTDeclareConstraint.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Parsers/MySQL/ASTDeclareReference.h>
#include <Parsers/ParserDataType.h>

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

static inline bool parseColumnDeclareOptions(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserDeclareOptions p_non_generate_options{
        {
            OptionDescribe("ZEROFILL", "zero_fill", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("SIGNED", "is_unsigned", std::make_unique<ParserAlwaysFalse>()),
            OptionDescribe("UNSIGNED", "is_unsigned", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("NULL", "is_null", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("NOT NULL", "is_null", std::make_unique<ParserAlwaysFalse>()),
            OptionDescribe("DEFAULT", "default", std::make_unique<ParserExpression>()),
            OptionDescribe("ON UPDATE", "on_update", std::make_unique<ParserExpression>()),
            OptionDescribe("AUTO_INCREMENT", "auto_increment", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("UNIQUE KEY", "unique_key", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("PRIMARY KEY", "primary_key", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("UNIQUE", "unique_key", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("KEY", "primary_key", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("COMMENT", "comment", std::make_unique<ParserStringLiteral>()),
            OptionDescribe("CHARACTER SET", "charset_name", std::make_unique<ParserCharsetOrCollateName>()),
            OptionDescribe("CHARSET", "charset", std::make_unique<ParserCharsetOrCollateName>()),
            OptionDescribe("COLLATE", "collate", std::make_unique<ParserCharsetOrCollateName>()),
            OptionDescribe("COLUMN_FORMAT", "column_format", std::make_unique<ParserIdentifier>()),
            OptionDescribe("STORAGE", "storage", std::make_unique<ParserIdentifier>()),
            OptionDescribe("AS", "generated", std::make_unique<ParserExpression>()),
            OptionDescribe("GENERATED ALWAYS AS", "generated", std::make_unique<ParserExpression>()),
            OptionDescribe("STORED", "is_stored", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("VIRTUAL", "is_stored", std::make_unique<ParserAlwaysFalse>()),
            OptionDescribe("INVISIBLE", "", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("", "reference", std::make_unique<ParserDeclareReference>()),
            OptionDescribe("", "constraint", std::make_unique<ParserDeclareConstraint>()),
        }
    };

    return p_non_generate_options.parse(pos, node, expected);
}

bool ParserDeclareColumn::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr column_name;
    ASTPtr column_data_type;
    ASTPtr column_options;

    ParserExpression p_expression;
    ParserIdentifier p_identifier;

    if (!p_identifier.parse(pos, column_name, expected))
        return false;

    if (!ParserDataType().parse(pos, column_data_type, expected))
        return false;

    parseColumnDeclareOptions(pos, column_options, expected);

    auto declare_column = std::make_shared<ASTDeclareColumn>();
    declare_column->name = getIdentifierName(column_name);
    declare_column->data_type = column_data_type;
    declare_column->column_options = column_options;

    if (declare_column->data_type)
        declare_column->children.emplace_back(declare_column->data_type);

    if (declare_column->column_options)
        declare_column->children.emplace_back(declare_column->column_options);

    node = declare_column;
    return true;
}

}

}
