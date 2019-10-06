#pragma once

#include <Parsers/IAST.h>
#include <Access/AccessPrivileges.h>
#include <unordered_map>


namespace DB
{
/** {GRANT | REVOKE}
  *     role [, role ...]
  *     {TO | FROM} user_or_role [, user_or_role...]
  *     [WITH ADMIN OPTION]
  *
  * {GRANT | REVOKE}
  *     {USAGE | SELECT | SELECT(columns) | INSERT | DELETE | ALTER | CREATE | DROP | ALL [PRIVILEGES]} [, ...]
  *     ON *.* | database.* | database.table | * | table
  *     {TO | FROM} user_or_role [, user_or_role ...]
  *     [WITH GRANT OPTION]
  */
class ASTGrantQuery : public IAST
{
public:
    /// We use ASTGrantQuery for both GRANT and REVOKE queries.
    enum class Kind
    {
        GRANT,
        REVOKE,
    };
    Kind kind = Kind::GRANT;

    Strings roles;

    String database;
    bool use_current_database = false;
    String table;

    using AccessType = AccessPrivileges::Type;
    AccessType access = AccessPrivileges::USAGE;
    std::unordered_map<String, AccessType> columns_access;

    Strings to_roles;
    bool grant_option = false;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
