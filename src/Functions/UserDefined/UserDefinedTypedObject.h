#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB
{

struct UserDefinedTypedObject
{
    ASTPtr object;
    UserDefinedSQLObjectType object_type;
};

}
