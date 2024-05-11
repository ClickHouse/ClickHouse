#include "TestStructure.h"

namespace DB::PostgreSQL::Testing
{
    Test SelectWithUnionAll{
        "SELECT 1 UNION ALL SELECT 2"
        ,
        "{\"version\":160001,\"stmts\":[{\"stmt\":{\"SelectStmt\":{\"limitOption\":\"LIMIT_OPTION_DEFAULT\",\"op\":\"SETOP_UNION\",\"all\":true,\"larg\":{\"targetList\":[{\"ResTarget\":{\"val\":{\"A_Const\":{\"ival\":{\"ival\":1},\"location\":7}},\"location\":7}}],\"limitOption\":\"LIMIT_OPTION_DEFAULT\",\"op\":\"SETOP_NONE\"},\"rarg\":{\"targetList\":[{\"ResTarget\":{\"val\":{\"A_Const\":{\"ival\":{\"ival\":2},\"location\":26}},\"location\":26}}],\"limitOption\":\"LIMIT_OPTION_DEFAULT\",\"op\":\"SETOP_NONE\"}}}}]}"
        ,
        "SelectWithUnionQuery (children 1) "
            "ExpressionList (children 2) "
                "SelectQuery (children 1) "
                    "ExpressionList (children 1) "
                        "Literal UInt64_1 "
                "SelectQuery (children 1) "
                    "ExpressionList (children 1) "
                        "Literal UInt64_2"
    };
}
