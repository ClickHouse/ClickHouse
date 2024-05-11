#include "TestStructure.h"

namespace DB::PostgreSQL::Testing
{
    Test SelectInt{
            "SELECT 1"
            ,
            "{\"version\":160001,\"stmts\":[{\"stmt\":{\"SelectStmt\":{\"targetList\":[{\"ResTarget\":{\"val\":{\"A_Const\":{\"ival\":{\"ival\":1},\"location\":7}},\"location\":7}}],\"limitOption\":\"LIMIT_OPTION_DEFAULT\",\"op\":\"SETOP_NONE\"}}}]}"
            ,
            "SelectWithUnionQuery (children 1) "
                "ExpressionList (children 1) "
                    "SelectQuery (children 1) "
                        "ExpressionList (children 1) "
                            "Literal UInt64_1"
    };

    Test SelectFloat{
        "SELECT 2.5"
        ,
        "{\"version\":160001,\"stmts\":[{\"stmt\":{\"SelectStmt\":{\"targetList\":[{\"ResTarget\":{\"val\":{\"A_Const\":{\"fval\":{\"fval\":\"2.5\"},\"location\":7}},\"location\":7}}],\"limitOption\":\"LIMIT_OPTION_DEFAULT\",\"op\":\"SETOP_NONE\"}}}]}"
        ,
        "SelectWithUnionQuery (children 1) "
            "ExpressionList (children 1) "
                "SelectQuery (children 1) "
                    "ExpressionList (children 1) "
                        "Literal Float64_2.5"
    };


    Test SelectBool{
        "SELECT TRUE"
        ,
        "{\"version\":160001,\"stmts\":[{\"stmt\":{\"SelectStmt\":{\"targetList\":[{\"ResTarget\":{\"val\":{\"A_Const\":{\"boolval\":{\"boolval\":true},\"location\":7}},\"location\":7}}],\"limitOption\":\"LIMIT_OPTION_DEFAULT\",\"op\":\"SETOP_NONE\"}}}]}"
        ,
        "SelectWithUnionQuery (children 1) "
            "ExpressionList (children 1) "
                "SelectQuery (children 1) "
                    "ExpressionList (children 1) "
                        "Literal Bool_1"
    };

    Test SelectAsteriskFromTable = {
        "SELECT "
        " * "
        "FROM users "
        ,
        "{\"version\":160001,\"stmts\":[{\"stmt\":{\"SelectStmt\":{\"targetList\":[{\"ResTarget\":{\"val\":{\"ColumnRef\":{\"fields\":[{\"A_Star\":{}}],\"location\":8}},\"location\":8}}],\"fromClause\":[{\"RangeVar\":{\"relname\":\"users\",\"inh\":true,\"relpersistence\":\"p\",\"location\":15}}],\"limitOption\":\"LIMIT_OPTION_DEFAULT\",\"op\":\"SETOP_NONE\"}}}]}"
        ,
        "SelectWithUnionQuery (children 1) "
            "ExpressionList (children 1) "
                "SelectQuery (children 2) "
                    "ExpressionList (children 1) "
                        "Asterisk "
                    "TablesInSelectQuery (children 1) "
                        "TablesInSelectQueryElement (children 1) "
                            "TableExpression (children 1) "
                                "TableIdentifier users "
    };
}
