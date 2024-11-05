#include <Parsers/MySQL/ASTAlterCommand.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Parsers/MySQL/ASTDeclareTableOptions.h>
#include <Interpreters/StorageID.h>

namespace DB
{

namespace MySQLParser
{

ASTPtr ASTAlterCommand::clone() const
{
    auto res = std::make_shared<ASTAlterCommand>(*this);
    res->children.clear();

    if (index_decl)
        res->set(res->index_decl, index_decl->clone());

    if (default_expression)
        res->set(res->default_expression, default_expression->clone());

    if (additional_columns)
        res->set(res->additional_columns, additional_columns->clone());

    if (order_by_columns)
        res->set(res->order_by_columns, order_by_columns->clone());

    if (properties)
        res->set(res->properties, properties->clone());

    return res;
}

static inline bool parseAddCommand(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr declare_index;
    ASTPtr additional_columns;
    ParserDeclareIndex index_p;
    ParserDeclareColumn column_p;

    auto alter_command = std::make_shared<ASTAlterCommand>();

    if (index_p.parse(pos, declare_index, expected))
    {
        alter_command->type = ASTAlterCommand::ADD_INDEX;
        alter_command->set(alter_command->index_decl, declare_index);
    }
    else
    {
        alter_command->type = ASTAlterCommand::ADD_COLUMN;
        ParserKeyword(Keyword::COLUMN).ignore(pos, expected);

        if (ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
        {
            ParserList columns_p(std::make_unique<ParserDeclareColumn>(), std::make_unique<ParserToken>(TokenType::Comma));

            if (!columns_p.parse(pos, additional_columns, expected))
                return false;

            if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
                return false;
        }
        else
        {
            ASTPtr declare_column;
            if (!column_p.parse(pos, declare_column, expected))
                return false;

            additional_columns = std::make_shared<ASTExpressionList>();
            additional_columns->children.emplace_back(declare_column);

            if (ParserKeyword(Keyword::FIRST).ignore(pos, expected))
                alter_command->first = true;
            else if (ParserKeyword(Keyword::AFTER).ignore(pos, expected))
            {
                ASTPtr after_column;
                ParserIdentifier identifier_p;
                if (!identifier_p.parse(pos, after_column, expected))
                    return false;

                alter_command->column_name = getIdentifierName(after_column);
            }
        }

        alter_command->set(alter_command->additional_columns, additional_columns);
    }

    node = alter_command;
    return true;
}

static inline bool parseDropCommand(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr name;
    ParserIdentifier identifier_p;

    auto alter_command = std::make_shared<ASTAlterCommand>();

    if (ParserKeyword(Keyword::PRIMARY_KEY).ignore(pos, expected))
    {
        alter_command->index_type = "PRIMARY_KEY";
        alter_command->type = ASTAlterCommand::DROP_INDEX;
    }
    else if (ParserKeyword(Keyword::FOREIGN_KEY).ignore(pos, expected))
    {
        if (!identifier_p.parse(pos, name, expected))
            return false;

        alter_command->index_type = "FOREIGN";
        alter_command->type = ASTAlterCommand::DROP_INDEX;
        alter_command->index_name = getIdentifierName(name);
    }
    else if (ParserKeyword(Keyword::INDEX).ignore(pos, expected) || ParserKeyword(Keyword::KEY).ignore(pos, expected))
    {
        if (!identifier_p.parse(pos, name, expected))
            return false;

        alter_command->index_type = "KEY";
        alter_command->type = ASTAlterCommand::DROP_INDEX;
        alter_command->index_name = getIdentifierName(name);
    }
    else if (ParserKeyword(Keyword::CONSTRAINT).ignore(pos, expected) || ParserKeyword(Keyword::CHECK).ignore(pos, expected))
    {
        if (!identifier_p.parse(pos, name, expected))
            return false;

        alter_command->type = ASTAlterCommand::DROP_CHECK;
        alter_command->constraint_name = getIdentifierName(name);
    }
    else
    {
        ParserKeyword(Keyword::COLUMN).ignore(pos, expected);

        if (!identifier_p.parse(pos, name, expected))
            return false;

        alter_command->type = ASTAlterCommand::DROP_COLUMN;
        alter_command->column_name = getIdentifierName(name);
    }

    node = alter_command;
    return true;
}

static inline bool parseAlterCommand(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr name;

    ParserIdentifier identifier_p;
    auto alter_command = std::make_shared<ASTAlterCommand>();

    if (ParserKeyword(Keyword::INDEX).ignore(pos, expected))
    {
        /// ALTER INDEX index_name {VISIBLE | INVISIBLE}

        if (!identifier_p.parse(pos, name, expected))
            return false;

        alter_command->index_visible = ParserKeyword(Keyword::VISIBLE).ignore(pos, expected);

        if (!alter_command->index_visible && !ParserKeyword(Keyword::INVISIBLE).ignore(pos, expected))
            return false;

        alter_command->type = ASTAlterCommand::MODIFY_INDEX_VISIBLE;
        alter_command->index_name = getIdentifierName(name);
    }
    else if (ParserKeyword(Keyword::CHECK).ignore(pos, expected) || ParserKeyword(Keyword::CONSTRAINT).ignore(pos, expected))
    {
        /// ALTER {CHECK | CONSTRAINT} symbol [NOT] ENFORCED
        if (!identifier_p.parse(pos, name, expected))
            return false;

        alter_command->not_check_enforced = ParserKeyword(Keyword::NOT).ignore(pos, expected);

        if (!ParserKeyword(Keyword::ENFORCED).ignore(pos, expected))
            return false;

        alter_command->type = ASTAlterCommand::MODIFY_CHECK;
        alter_command->constraint_name = getIdentifierName(name);
    }
    else
    {
        /// ALTER [COLUMN] col_name {SET DEFAULT {literal | (expr)} | DROP DEFAULT}

        ParserKeyword(Keyword::COLUMN).ignore(pos, expected);

        if (!identifier_p.parse(pos, name, expected))
            return false;

        if (ParserKeyword(Keyword::DROP_DEFAULT).ignore(pos, expected))
            alter_command->type = ASTAlterCommand::DROP_COLUMN_DEFAULT;
        else if (ParserKeyword(Keyword::SET_DEFAULT).ignore(pos, expected))
        {
            ASTPtr default_expression;
            ParserExpression expression_p;

            if (!expression_p.parse(pos, default_expression, expected))
                return false;

            alter_command->type = ASTAlterCommand::MODIFY_COLUMN_DEFAULT;
            alter_command->set(alter_command->default_expression, default_expression);
        }
        else
            return false;

        alter_command->column_name = getIdentifierName(name);
    }

    node = alter_command;
    return true;
}

static inline bool parseRenameCommand(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr old_name;
    ASTPtr new_name;

    ParserIdentifier identifier_p;
    auto alter_command = std::make_shared<ASTAlterCommand>();

    if (ParserKeyword(Keyword::COLUMN).ignore(pos, expected))
    {
        if (!identifier_p.parse(pos, old_name, expected))
            return false;

        if (!ParserKeyword(Keyword::TO).ignore(pos, expected))
            return false;

        if (!identifier_p.parse(pos, new_name, expected))
            return false;

        alter_command->type = ASTAlterCommand::RENAME_COLUMN;
        alter_command->old_name = getIdentifierName(old_name);
        alter_command->column_name = getIdentifierName(new_name);
    }
    else if (ParserKeyword(Keyword::TO).ignore(pos, expected) || ParserKeyword(Keyword::AS).ignore(pos, expected))
    {
        if (!ParserCompoundIdentifier(true).parse(pos, new_name, expected))
            return false;

        auto new_table_id = new_name->as<ASTTableIdentifier>()->getTableId();
        alter_command->type = ASTAlterCommand::RENAME_TABLE;
        alter_command->new_table_name = new_table_id.table_name;
        alter_command->new_database_name = new_table_id.database_name;
    }
    else if (ParserKeyword(Keyword::INDEX).ignore(pos, expected) || ParserKeyword(Keyword::KEY).ignore(pos, expected))
    {
        if (!identifier_p.parse(pos, old_name, expected))
            return false;

        if (!ParserKeyword(Keyword::TO).ignore(pos, expected))
            return false;

        if (!identifier_p.parse(pos, new_name, expected))
            return false;

        alter_command->type = ASTAlterCommand::RENAME_INDEX;
        alter_command->old_name = getIdentifierName(old_name);
        alter_command->index_name = getIdentifierName(new_name);
    }
    else
    {
        if (!ParserCompoundIdentifier(true).parse(pos, new_name, expected))
            return false;
        auto new_table_id = new_name->as<ASTTableIdentifier>()->getTableId();
        alter_command->type = ASTAlterCommand::RENAME_TABLE;
        alter_command->new_table_name = new_table_id.table_name;
        alter_command->new_database_name = new_table_id.database_name;
    }

    node = alter_command;
    return true;
}

static inline bool parseOtherCommand(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    auto alter_command = std::make_shared<ASTAlterCommand>();

    if (ParserKeyword(Keyword::ORDER_BY).ignore(pos, expected))
    {
        /// ORDER BY col_name [, col_name] ...
        ASTPtr columns;
        ParserList columns_p(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma));

        if (!columns_p.parse(pos, columns, expected))
            return false;

        alter_command->type = ASTAlterCommand::ORDER_BY;
        alter_command->set(alter_command->order_by_columns, columns);
    }
    else
    {
        ParserDeclareOption options_p{
            {
                OptionDescribe("FORCE", "force", std::make_shared<ParserAlwaysTrue>()),
                OptionDescribe("ALGORITHM", "algorithm", std::make_shared<ParserIdentifier>()),
                OptionDescribe("WITH VALIDATION", "validation", std::make_shared<ParserAlwaysTrue>()),
                OptionDescribe("WITHOUT VALIDATION", "validation", std::make_shared<ParserAlwaysFalse>()),
                OptionDescribe("IMPORT TABLESPACE", "import_tablespace", std::make_shared<ParserAlwaysTrue>()),
                OptionDescribe("DISCARD TABLESPACE", "import_tablespace", std::make_shared<ParserAlwaysFalse>()),
                OptionDescribe("ENABLE KEYS", "enable_keys", std::make_shared<ParserAlwaysTrue>()),
                OptionDescribe("DISABLE KEYS", "enable_keys", std::make_shared<ParserAlwaysFalse>()),
                /// TODO: with collate
                OptionDescribe("CONVERT TO CHARACTER SET", "charset", std::make_shared<ParserCharsetOrCollateName>()),
                OptionDescribe("CHARACTER SET", "charset", std::make_shared<ParserCharsetOrCollateName>()),
                OptionDescribe("DEFAULT CHARACTER SET", "charset", std::make_shared<ParserCharsetOrCollateName>()),
                OptionDescribe("COMMENT", "", std::make_shared<ParserIdentifier>()),
                OptionDescribe("LOCK", "lock", std::make_shared<ParserIdentifier>())
            }
        };

        ASTPtr properties_options;
        ParserDeclareTableOptions table_options_p;

        if (!options_p.parse(pos, properties_options, expected) && !table_options_p.parse(pos, properties_options, expected))
            return false;

        alter_command->type = ASTAlterCommand::MODIFY_PROPERTIES;
        alter_command->set(alter_command->properties, properties_options);
    }

    node = alter_command;
    return true;
}

static inline bool parseModifyCommand(IParser::Pos & pos, ASTPtr & node, Expected & expected, bool exists_old_column_name = false)
{
    ASTPtr old_column_name;
    auto alter_command = std::make_shared<ASTAlterCommand>();

    ParserKeyword(Keyword::COLUMN).ignore(pos, expected);
    if (exists_old_column_name && !ParserIdentifier().parse(pos, old_column_name, expected))
        return false;

    ASTPtr additional_column;
    if (!ParserDeclareColumn().parse(pos, additional_column, expected))
        return false;

    if (ParserKeyword(Keyword::FIRST).ignore(pos, expected))
        alter_command->first = true;
    else if (ParserKeyword(Keyword::AFTER).ignore(pos, expected))
    {
        ASTPtr after_column;
        ParserIdentifier identifier_p;
        if (!identifier_p.parse(pos, after_column, expected))
            return false;

        alter_command->column_name = getIdentifierName(after_column);
    }

    node = alter_command;
    alter_command->type = ASTAlterCommand::MODIFY_COLUMN;
    alter_command->set(alter_command->additional_columns, std::make_shared<ASTExpressionList>());
    alter_command->additional_columns->children.emplace_back(additional_column);

    if (exists_old_column_name)
        alter_command->old_name = getIdentifierName(old_column_name);

    return true;
}

bool ParserAlterCommand::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword k_add(Keyword::ADD);
    ParserKeyword k_drop(Keyword::DROP);
    ParserKeyword k_alter(Keyword::ALTER);
    ParserKeyword k_rename(Keyword::RENAME);
    ParserKeyword k_modify(Keyword::MODIFY);
    ParserKeyword k_change(Keyword::CHANGE);

    if (k_add.ignore(pos, expected))
        return parseAddCommand(pos, node, expected);
    if (k_drop.ignore(pos, expected))
        return parseDropCommand(pos, node, expected);
    if (k_alter.ignore(pos, expected))
        return parseAlterCommand(pos, node, expected);
    if (k_rename.ignore(pos, expected))
        return parseRenameCommand(pos, node, expected);
    if (k_modify.ignore(pos, expected))
        return parseModifyCommand(pos, node, expected);
    if (k_change.ignore(pos, expected))
        return parseModifyCommand(pos, node, expected, true);
    return parseOtherCommand(pos, node, expected);
}
}

}
