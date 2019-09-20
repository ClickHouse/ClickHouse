#pragma once

#include <Parsers/IAST.h>


namespace DB
{
/// ASTGrantQuery is used to represent GRANT or REVOKE query.
/// Syntax:
/// {GRANT | REVOKE}
///     role [, role ...]
///     {TO | FROM} user_or_role [, user_or_role...]
///     [WITH ADMIN OPTION]
///
/// {GRANT | REVOKE}
///     {USAGE | SELECT | SELECT(columns) | INSERT | DELETE | ALTER | CREATE | DROP | ALL [PRIVILEGES]} [, ...]
///     ON *.* | database.* | database.table | * | table
///     {TO | FROM} user_or_role [, user_or_role ...]
///     [WITH GRANT OPTION]
class ASTGrantQuery : public IAST
{
public:
    enum class Kind
    {
        GRANT,
        REVOKE,
    };
    Kind kind = Kind::GRANT;

    std::vector<String> roles;

    String database;
    bool use_current_database = false;
    String table;

    enum AccessType
    {
        SELECT = 0x01,
        INSERT = 0x02,
        DELETE = 0x04,
        ALTER = 0x08,
        CREATE = 0x10,
        DROP = 0x20,
    };
    int access = 0;

    std::vector<String> columns;
    int columns_access = 0;

    std::vector<String> to_roles;
    bool with_grant_option = false;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
