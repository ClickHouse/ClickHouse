#pragma once
#include <Core/Types.h>


/// SQL security enum. Used in ASTSQLSecurity::type. For more info, please refer to the docs/sql-reference/statements/create/view.md#sql_security
enum class SQLSecurityType : uint8_t
{
    INVOKER,  /// All queries will be executed with the current user's context.
    DEFINER, /// All queries will be executed with the specified user's context.
    NONE, /// All queries will be executed with the global context.
};
