#include "TestStructure.h"

namespace DB::PostgreSQL::Testing
{
    Test CreateTable{
        "CREATE TABLE students ( "
            "id INT PRIMARY KEY, "
            "name VARCHAR(100), "
            "age INT "
        ")"
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