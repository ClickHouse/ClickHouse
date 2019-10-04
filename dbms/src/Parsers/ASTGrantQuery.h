#pragma once

#include <Parsers/IAST.h>
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
        ALL_COLUMN_LEVEL = SELECT,
        ALL_TABLE_LEVEL = ALL_COLUMN_LEVEL | INSERT | DELETE | ALTER | DROP,
        ALL_DATABASE_LEVEL = ALL_TABLE_LEVEL | CREATE,
        ALL = ALL_DATABASE_LEVEL,
    };

    static const std::vector<std::pair<AccessTypes, String>> & getAccessTypeNames();

    /// Outputs a grant to string in readable format, for example "SELECT(column), INSERT ON mydatabase.*".
    static String accessTypeToString(AccessType access_);
    static String accessToString(AccessType access_);
    static String accessToString(AccessType access_, const String & database_);
    static String accessToString(AccessType access_, const String & database_, const String & table_);
    static String accessToString(AccessType access_, const String & database_, const String & table_, const String & column_);
    static String accessToString(AccessType access_, const String & database_, const String & table_, const Strings & columns_);

    AccessType access = USAGE;
    std::unordered_map<String, AccessType> columns_access;

    Strings to_roles;
    bool grant_option = false;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
