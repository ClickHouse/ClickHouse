#include "TestStructure.h"

namespace DB::PostgreSQL::Testing
{
    Test InsertIntoSelect{
        "INSERT INTO users (id) SELECT 1"
        ,
        "{\"version\":160001,\"stmts\":[{\"stmt\":{\"InsertStmt\":{\"relation\":{\"relname\":\"users\",\"inh\":true,\"relpersistence\":\"p\",\"location\":12},\"cols\":[{\"ResTarget\":{\"name\":\"id\",\"location\":19}}],\"selectStmt\":{\"SelectStmt\":{\"targetList\":[{\"ResTarget\":{\"val\":{\"A_Const\":{\"ival\":{\"ival\":1},\"location\":30}},\"location\":30}}],\"limitOption\":\"LIMIT_OPTION_DEFAULT\",\"op\":\"SETOP_NONE\"}},\"override\":\"OVERRIDING_NOT_SET\"}}}]}"
        ,
        "InsertQuery   (children 3) "
            "Identifier users "
            "ExpressionList (children 1) "
                "Identifier id "
            "SelectWithUnionQuery (children 1) "
            "ExpressionList (children 1) "
            "SelectQuery (children 1) "
                "ExpressionList (children 1) "
                "Literal UInt64_1"
    };
}
