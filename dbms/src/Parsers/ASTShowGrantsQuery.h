#pragma once

#include <Parsers/IAST.h>
#include <unordered_map>


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
class ASTShowGrantsQuery : public IAST
{
public:
    std::vector<String> roles;

    String database;
    bool use_current_database = false;
    String table;

    using AccessType = int;
    enum AccessTypes : AccessType
    {
        USAGE = 0x00,
        SELECT = 0x01,
        INSERT = 0x02,
        DELETE = 0x04,
        ALTER = 0x08,
        CREATE = 0x10,
        DROP = 0x20,
        ALL = -1,
    };

    static const std::vector<std::pair<AccessTypes, String>> & getAccessTypeNames();

    AccessType access = USAGE;
    std::unordered_map<String, AccessType> columns_access;

    std::vector<String> to_roles;
    bool grant_option = false;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
