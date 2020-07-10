#pragma once

#include <Parsers/ASTExpressionList.h>
#include <Parsers/IAST.h>
#include <Parsers/MySQL/ASTDeclareColumn.h>
#include <Parsers/MySQL/ASTDeclareIndex.h>

namespace DB
{


namespace MySQLParser
{

class ASTAlterCommand : public IAST
{
public:
    enum Type
    {
        ADD_INDEX,
        ADD_COLUMN,

        DROP_INDEX,
        DROP_CHECK,
        DROP_COLUMN,
        DROP_COLUMN_DEFAULT,

        RENAME_INDEX,
        RENAME_COLUMN,
        RENAME_FOREIGN,

        MODIFY_CHECK,
        MODIFY_COLUMN,
        MODIFY_TABLE_OPTIONS,
        MODIFY_INDEX_VISIBLE,
        MODIFY_COLUMN_DEFAULT,

        ORDER_BY,

        NO_TYPE
    };

    Type type = NO_TYPE;

    /// For ADD INDEX
    ASTDeclareIndex * index_decl;

    /// For modify default expression
    IAST * default_expression;

    /// For ADD COLUMN
    ASTExpressionList * additional_columns;

    bool first = false;
    bool index_visible = false;
    bool not_check_enforced = false;

    String old_name;
    String index_type;
    String index_name;
    String column_name;
    String constraint_name;
};

class ParserAlterCommand : public IParserBase
{
protected:
    const char * getName() const override { return "alter command"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    bool parseAddCommand(Pos & pos, ASTPtr & node, Expected & expected);

    bool parseDropCommand(Pos & pos, ASTPtr & node, Expected & expected);

    bool parseAlterCommand(Pos & pos, ASTPtr & node, Expected & expected);

    bool parseRenameCommand(Pos & pos, ASTPtr & node, Expected & expected);

    bool parseOtherCommand(Pos & pos, ASTPtr & node, Expected & expected);
};

}

}
