#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB
{

struct UserDefinedSQLTypedObject
{
    ASTPtr object;
    UserDefinedSQLObjectType object_type;
};

}
