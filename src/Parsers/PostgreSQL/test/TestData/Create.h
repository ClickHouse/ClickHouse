#include "TestStructure.h"

namespace DB::PostgreSQL::Testing
{
    Test CreateTable{
        "CREATE TABLE students ( "
            "id INT, "
            "name TEXT, "
            "age INT "
        ")"
        ,
        "{\"version\":160001,\"stmts\":[{\"stmt\":{\"CreateStmt\":{\"relation\":{\"relname\":\"students\",\"inh\":true,\"relpersistence\":\"p\",\"location\":13},\"tableElts\":[{\"ColumnDef\":{\"colname\":\"id\",\"typeName\":{\"names\":[{\"String\":{\"sval\":\"pg_catalog\"}},{\"String\":{\"sval\":\"int4\"}}],\"typemod\":-1,\"location\":27},\"is_local\":true,\"location\":24}},{\"ColumnDef\":{\"colname\":\"name\",\"typeName\":{\"names\":[{\"String\":{\"sval\":\"text\"}}],\"typemod\":-1,\"location\":37},\"is_local\":true,\"location\":32}},{\"ColumnDef\":{\"colname\":\"age\",\"typeName\":{\"names\":[{\"String\":{\"sval\":\"pg_catalog\"}},{\"String\":{\"sval\":\"int4\"}}],\"typemod\":-1,\"location\":47},\"is_local\":true,\"location\":43}}],\"oncommit\":\"ONCOMMIT_NOOP\"}}}]}"
        ,
        "CreateQuery  students (children 2) "
        "Identifier students "
        "Columns definition (children 1) "
            "ExpressionList (children 3) "
                "ColumnDeclaration id (children 1) "
                    "Function INT "
                "ColumnDeclaration name (children 1) "
                    "Function TEXT "
                "ColumnDeclaration age (children 1) "
                    "Function INT"
    };

    Test CreateTablePrimaryKey{
        "CREATE TABLE students ( "
            "id INT PRIMARY KEY, "
            "name VARCHAR(100), "
            "age INT "
        ")"
        ,
        "{\"version\":160001,\"stmts\":[{\"stmt\":{\"CreateStmt\":{\"relation\":{\"relname\":\"students\",\"inh\":true,\"relpersistence\":\"p\",\"location\":13},\"tableElts\":[{\"ColumnDef\":{\"colname\":\"id\",\"typeName\":{\"names\":[{\"String\":{\"sval\":\"pg_catalog\"}},{\"String\":{\"sval\":\"int4\"}}],\"typemod\":-1,\"location\":29},\"is_local\":true,\"constraints\":[{\"Constraint\":{\"contype\":\"CONSTR_PRIMARY\",\"location\":33}}],\"location\":26}},{\"ColumnDef\":{\"colname\":\"name\",\"typeName\":{\"names\":[{\"String\":{\"sval\":\"pg_catalog\"}},{\"String\":{\"sval\":\"varchar\"}}],\"typmods\":[{\"A_Const\":{\"ival\":{\"ival\":100},\"location\":61}}],\"typemod\":-1,\"location\":53},\"is_local\":true,\"location\":48}},{\"ColumnDef\":{\"colname\":\"age\",\"typeName\":{\"names\":[{\"String\":{\"sval\":\"pg_catalog\"}},{\"String\":{\"sval\":\"int4\"}}],\"typemod\":-1,\"location\":73},\"is_local\":true,\"location\":69}}],\"oncommit\":\"ONCOMMIT_NOOP\"}}}]}"
        ,
        "CreateQuery  students (children 3) "
            "Identifier students "
            "Columns definition (children 2) "
                "ExpressionList (children 3) "
                    "ColumnDeclaration id (children 1) "
                        "Function INT "
            "ColumnDeclaration name (children 1) "
                "Function VARCHAR (children 1) "
                    "ExpressionList (children 1) "
                        "Literal UInt64_100 "
            "ColumnDeclaration age (children 1) "
                "Function INT "
        "Function tuple (children 1) "
            "ExpressionList (children 1) "
            "Identifier id "
    "Storage definition "
    };
}