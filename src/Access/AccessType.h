#pragma once

#include <Core/Types.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <array>


namespace DB
{
/// Represents an access type which can be granted on databases, tables, columns, etc.
enum class AccessType
{
/// Macro M should be defined as M(name, aliases, node_type, parent_group_name)
/// where name is identifier with underscores (instead of spaces);
/// aliases is a string containing comma-separated list;
/// node_type either specifies access type's level (GLOBAL/DATABASE/TABLE/DICTIONARY/VIEW/COLUMNS),
/// or specifies that the access type is a GROUP of other access types;
/// parent_group_name is the name of the group containing this access type (or NONE if there is no such group).
#define APPLY_FOR_ACCESS_TYPES(M) \
    M(SHOW_DATABASES, "", DATABASE, SHOW) /* allows to execute SHOW DATABASES, SHOW CREATE DATABASE, USE <database>;
                                             implicitly enabled by any grant on the database */\
    M(SHOW_TABLES, "", TABLE, SHOW) /* allows to execute SHOW TABLES, EXISTS <table>, CHECK <table>;
                                       implicitly enabled by any grant on the table */\
    M(SHOW_COLUMNS, "", COLUMN, SHOW) /* allows to execute SHOW CREATE TABLE, DESCRIBE;
                                         implicitly enabled with any grant on the column */\
    M(SHOW_DICTIONARIES, "", DICTIONARY, SHOW) /* allows to execute SHOW DICTIONARIES, SHOW CREATE DICTIONARY, EXISTS <dictionary>;
                                                  implicitly enabled by any grant on the dictionary */\
    M(SHOW, "", GROUP, ALL) /* allows to execute SHOW, USE, EXISTS, CHECK, DESCRIBE */\
    \
    M(SELECT, "", COLUMN, ALL) \
    M(INSERT, "", COLUMN, ALL) \
    M(ALTER_UPDATE, "UPDATE", COLUMN, ALTER_TABLE) /* allows to execute ALTER UPDATE */\
    M(ALTER_DELETE, "DELETE", COLUMN, ALTER_TABLE) /* allows to execute ALTER DELETE */\
    \
    M(ALTER_ADD_COLUMN, "ADD COLUMN", COLUMN, ALTER_COLUMN) \
    M(ALTER_MODIFY_COLUMN, "MODIFY COLUMN", COLUMN, ALTER_COLUMN) \
    M(ALTER_DROP_COLUMN, "DROP COLUMN", COLUMN, ALTER_COLUMN) \
    M(ALTER_COMMENT_COLUMN, "COMMENT COLUMN", COLUMN, ALTER_COLUMN) \
    M(ALTER_CLEAR_COLUMN, "CLEAR COLUMN", COLUMN, ALTER_COLUMN) \
    M(ALTER_RENAME_COLUMN, "RENAME COLUMN", COLUMN, ALTER_COLUMN) \
    M(ALTER_COLUMN, "", GROUP, ALTER_TABLE) /* allow to execute ALTER {ADD|DROP|MODIFY...} COLUMN */\
    \
    M(ALTER_ORDER_BY, "ALTER MODIFY ORDER BY, MODIFY ORDER BY", TABLE, ALTER_INDEX) \
    M(ALTER_ADD_INDEX, "ADD INDEX", TABLE, ALTER_INDEX) \
    M(ALTER_DROP_INDEX, "DROP INDEX", TABLE, ALTER_INDEX) \
    M(ALTER_MATERIALIZE_INDEX, "MATERIALIZE INDEX", TABLE, ALTER_INDEX) \
    M(ALTER_CLEAR_INDEX, "CLEAR INDEX", TABLE, ALTER_INDEX) \
    M(ALTER_INDEX, "INDEX", GROUP, ALTER_TABLE) /* allows to execute ALTER ORDER BY or ALTER {ADD|DROP...} INDEX */\
    \
    M(ALTER_ADD_CONSTRAINT, "ADD CONSTRAINT", TABLE, ALTER_CONSTRAINT) \
    M(ALTER_DROP_CONSTRAINT, "DROP CONSTRAINT", TABLE, ALTER_CONSTRAINT) \
    M(ALTER_CONSTRAINT, "CONSTRAINT", GROUP, ALTER_TABLE) /* allows to execute ALTER {ADD|DROP} CONSTRAINT */\
    \
    M(ALTER_TTL, "ALTER MODIFY TTL, MODIFY TTL", TABLE, ALTER_TABLE) /* allows to execute ALTER MODIFY TTL */\
    M(ALTER_MATERIALIZE_TTL, "MATERIALIZE TTL", TABLE, ALTER_TABLE) /* allows to execute ALTER MATERIALIZE TTL;
                                                                       enabled implicitly by the grant ALTER_TABLE */\
    M(ALTER_SETTINGS, "ALTER SETTING, ALTER MODIFY SETTING, MODIFY SETTING", TABLE, ALTER_TABLE) /* allows to execute ALTER MODIFY SETTING */\
    M(ALTER_MOVE_PARTITION, "ALTER MOVE PART, MOVE PARTITION, MOVE PART", TABLE, ALTER_TABLE) \
    M(ALTER_FETCH_PARTITION, "FETCH PARTITION", TABLE, ALTER_TABLE) \
    M(ALTER_FREEZE_PARTITION, "FREEZE PARTITION", TABLE, ALTER_TABLE) \
    \
    M(ALTER_TABLE, "", GROUP, ALTER) \
    \
    M(ALTER_VIEW_REFRESH, "ALTER LIVE VIEW REFRESH, REFRESH VIEW", VIEW, ALTER_VIEW) \
    M(ALTER_VIEW_MODIFY_QUERY, "ALTER TABLE MODIFY QUERY", VIEW, ALTER_VIEW) \
    M(ALTER_VIEW, "", GROUP, ALTER) /* allows to execute ALTER VIEW REFRESH, ALTER VIEW MODIFY QUERY;
                                       implicitly enabled by the grant ALTER_TABLE */\
    \
    M(ALTER, "", GROUP, ALL) /* allows to execute ALTER {TABLE|LIVE VIEW} */\
    \
    M(CREATE_DATABASE, "", DATABASE, CREATE) /* allows to execute {CREATE|ATTACH} DATABASE */\
    M(CREATE_TABLE, "", TABLE, CREATE) /* allows to execute {CREATE|ATTACH} {TABLE|VIEW} */\
    M(CREATE_VIEW, "", VIEW, CREATE) /* allows to execute {CREATE|ATTACH} VIEW;
                                        implicitly enabled by the grant CREATE_TABLE */\
    M(CREATE_DICTIONARY, "", DICTIONARY, CREATE) /* allows to execute {CREATE|ATTACH} DICTIONARY */\
    M(CREATE_TEMPORARY_TABLE, "", GLOBAL, CREATE) /* allows to create and manipulate temporary tables;
                                                     implicitly enabled by the grant CREATE_TABLE on any table */ \
    M(CREATE, "", GROUP, ALL) /* allows to execute {CREATE|ATTACH} */ \
    \
    M(DROP_DATABASE, "", DATABASE, DROP) /* allows to execute {DROP|DETACH} DATABASE */\
    M(DROP_TABLE, "", TABLE, DROP) /* allows to execute {DROP|DETACH} TABLE */\
    M(DROP_VIEW, "", VIEW, DROP) /* allows to execute {DROP|DETACH} TABLE for views;
                                    implicitly enabled by the grant DROP_TABLE */\
    M(DROP_DICTIONARY, "", DICTIONARY, DROP) /* allows to execute {DROP|DETACH} DICTIONARY */\
    M(DROP, "", GROUP, ALL) /* allows to execute {DROP|DETACH} */\
    \
    M(TRUNCATE, "TRUNCATE TABLE", TABLE, ALL) \
    M(OPTIMIZE, "OPTIMIZE TABLE", TABLE, ALL) \
    \
    M(KILL_QUERY, "", GLOBAL, ALL) /* allows to kill a query started by another user
                                      (anyone can kill his own queries) */\
    \
    M(CREATE_USER, "", GLOBAL, ACCESS_MANAGEMENT) \
    M(ALTER_USER, "", GLOBAL, ACCESS_MANAGEMENT) \
    M(DROP_USER, "", GLOBAL, ACCESS_MANAGEMENT) \
    M(CREATE_ROLE, "", GLOBAL, ACCESS_MANAGEMENT) \
    M(ALTER_ROLE, "", GLOBAL, ACCESS_MANAGEMENT) \
    M(DROP_ROLE, "", GLOBAL, ACCESS_MANAGEMENT) \
    M(ROLE_ADMIN, "", GLOBAL, ACCESS_MANAGEMENT) /* allows to grant and revoke the roles which are not granted to the current user with admin option */\
    M(CREATE_ROW_POLICY, "CREATE POLICY", GLOBAL, ACCESS_MANAGEMENT) \
    M(ALTER_ROW_POLICY, "ALTER POLICY", GLOBAL, ACCESS_MANAGEMENT) \
    M(DROP_ROW_POLICY, "DROP POLICY", GLOBAL, ACCESS_MANAGEMENT) \
    M(CREATE_QUOTA, "", GLOBAL, ACCESS_MANAGEMENT) \
    M(ALTER_QUOTA, "", GLOBAL, ACCESS_MANAGEMENT) \
    M(DROP_QUOTA, "", GLOBAL, ACCESS_MANAGEMENT) \
    M(CREATE_SETTINGS_PROFILE, "CREATE PROFILE", GLOBAL, ACCESS_MANAGEMENT) \
    M(ALTER_SETTINGS_PROFILE, "ALTER PROFILE", GLOBAL, ACCESS_MANAGEMENT) \
    M(DROP_SETTINGS_PROFILE, "DROP PROFILE", GLOBAL, ACCESS_MANAGEMENT) \
    M(SHOW_USERS, "SHOW CREATE USER", GLOBAL, SHOW_ACCESS) \
    M(SHOW_ROLES, "SHOW CREATE ROLE", GLOBAL, SHOW_ACCESS) \
    M(SHOW_ROW_POLICIES, "SHOW POLICIES, SHOW CREATE ROW POLICY, SHOW CREATE POLICY", GLOBAL, SHOW_ACCESS) \
    M(SHOW_QUOTAS, "SHOW CREATE QUOTA", GLOBAL, SHOW_ACCESS) \
    M(SHOW_SETTINGS_PROFILES, "SHOW PROFILES, SHOW CREATE SETTINGS PROFILE, SHOW CREATE PROFILE", GLOBAL, SHOW_ACCESS) \
    M(SHOW_ACCESS, "", GROUP, ACCESS_MANAGEMENT) \
    M(ACCESS_MANAGEMENT, "", GROUP, ALL) \
    \
    M(SYSTEM_SHUTDOWN, "SYSTEM KILL, SHUTDOWN", GLOBAL, SYSTEM) \
    M(SYSTEM_DROP_DNS_CACHE, "SYSTEM DROP DNS, DROP DNS CACHE, DROP DNS", GLOBAL, SYSTEM_DROP_CACHE) \
    M(SYSTEM_DROP_MARK_CACHE, "SYSTEM DROP MARK, DROP MARK CACHE, DROP MARKS", GLOBAL, SYSTEM_DROP_CACHE) \
    M(SYSTEM_DROP_UNCOMPRESSED_CACHE, "SYSTEM DROP UNCOMPRESSED, DROP UNCOMPRESSED CACHE, DROP UNCOMPRESSED", GLOBAL, SYSTEM_DROP_CACHE) \
    M(SYSTEM_DROP_COMPILED_EXPRESSION_CACHE, "SYSTEM DROP COMPILED EXPRESSION, DROP COMPILED EXPRESSION CACHE, DROP COMPILED EXPRESSIONS", GLOBAL, SYSTEM_DROP_CACHE) \
    M(SYSTEM_DROP_CACHE, "DROP CACHE", GROUP, SYSTEM) \
    M(SYSTEM_RELOAD_CONFIG, "RELOAD CONFIG", GLOBAL, SYSTEM_RELOAD) \
    M(SYSTEM_RELOAD_DICTIONARY, "SYSTEM RELOAD DICTIONARIES, RELOAD DICTIONARY, RELOAD DICTIONARIES", GLOBAL, SYSTEM_RELOAD) \
    M(SYSTEM_RELOAD_EMBEDDED_DICTIONARIES, "RELOAD EMBEDDED DICTIONARIES", GLOBAL, SYSTEM_RELOAD) /* implicitly enabled by the grant SYSTEM_RELOAD_DICTIONARY ON *.* */\
    M(SYSTEM_RELOAD, "", GROUP, SYSTEM) \
    M(SYSTEM_MERGES, "SYSTEM STOP MERGES, SYSTEM START MERGES, STOP_MERGES, START MERGES", TABLE, SYSTEM) \
    M(SYSTEM_TTL_MERGES, "SYSTEM STOP TTL MERGES, SYSTEM START TTL MERGES, STOP TTL MERGES, START TTL MERGES", TABLE, SYSTEM) \
    M(SYSTEM_FETCHES, "SYSTEM STOP FETCHES, SYSTEM START FETCHES, STOP FETCHES, START FETCHES", TABLE, SYSTEM) \
    M(SYSTEM_MOVES, "SYSTEM STOP MOVES, SYSTEM START MOVES, STOP MOVES, START MOVES", TABLE, SYSTEM) \
    M(SYSTEM_DISTRIBUTED_SENDS, "SYSTEM STOP DISTRIBUTED SENDS, SYSTEM START DISTRIBUTED SENDS, STOP DISTRIBUTED SENDS, START DISTRIBUTED SENDS", TABLE, SYSTEM_SENDS) \
    M(SYSTEM_REPLICATED_SENDS, "SYSTEM STOP REPLICATED SENDS, SYSTEM START REPLICATED SENDS, STOP_REPLICATED_SENDS, START REPLICATED SENDS", TABLE, SYSTEM_SENDS) \
    M(SYSTEM_SENDS, "SYSTEM STOP SENDS, SYSTEM START SENDS, STOP SENDS, START SENDS", GROUP, SYSTEM) \
    M(SYSTEM_REPLICATION_QUEUES, "SYSTEM STOP REPLICATION QUEUES, SYSTEM START REPLICATION QUEUES, STOP_REPLICATION_QUEUES, START REPLICATION QUEUES", TABLE, SYSTEM) \
    M(SYSTEM_DROP_REPLICA, "DROP REPLICA", TABLE, SYSTEM) \
    M(SYSTEM_SYNC_REPLICA, "SYNC REPLICA", TABLE, SYSTEM) \
    M(SYSTEM_RESTART_REPLICA, "RESTART REPLICA", TABLE, SYSTEM) \
    M(SYSTEM_FLUSH_DISTRIBUTED, "FLUSH DISTRIBUTED", TABLE, SYSTEM_FLUSH) \
    M(SYSTEM_FLUSH_LOGS, "FLUSH LOGS", GLOBAL, SYSTEM_FLUSH) \
    M(SYSTEM_FLUSH, "", GROUP, SYSTEM) \
    M(SYSTEM, "", GROUP, ALL) /* allows to execute SYSTEM {SHUTDOWN|RELOAD CONFIG|...} */ \
    \
    M(dictGet, "dictHas, dictGetHierarchy, dictIsIn", DICTIONARY, ALL) /* allows to execute functions dictGet(), dictHas(), dictGetHierarchy(), dictIsIn() */\
    \
    M(addressToLine, "", GLOBAL, INTROSPECTION) /* allows to execute function addressToLine() */\
    M(addressToSymbol, "", GLOBAL, INTROSPECTION) /* allows to execute function addressToSymbol() */\
    M(demangle, "", GLOBAL, INTROSPECTION) /* allows to execute function demangle() */\
    M(INTROSPECTION, "INTROSPECTION FUNCTIONS", GROUP, ALL) /* allows to execute functions addressToLine(), addressToSymbol(), demangle()*/\
    \
    M(FILE, "", GLOBAL, SOURCES) \
    M(URL, "", GLOBAL, SOURCES) \
    M(REMOTE, "", GLOBAL, SOURCES) \
    M(MONGO, "", GLOBAL, SOURCES) \
    M(MYSQL, "", GLOBAL, SOURCES) \
    M(ODBC, "", GLOBAL, SOURCES) \
    M(JDBC, "", GLOBAL, SOURCES) \
    M(HDFS, "", GLOBAL, SOURCES) \
    M(S3, "", GLOBAL, SOURCES) \
    M(SOURCES, "", GROUP, ALL) \
    \
    M(ALL, "ALL PRIVILEGES", GROUP, NONE) /* full access */ \
    M(NONE, "USAGE, NO PRIVILEGES", GROUP, NONE) /* no access */

#define DECLARE_ACCESS_TYPE_ENUM_CONST(name, aliases, node_type, parent_group_name) \
    name,

    APPLY_FOR_ACCESS_TYPES(DECLARE_ACCESS_TYPE_ENUM_CONST)
#undef DECLARE_ACCESS_TYPE_ENUM_CONST
};


namespace impl
{
    template <typename = void>
    class AccessTypeToStringConverter
    {
    public:
        static const AccessTypeToStringConverter & instance()
        {
            static const AccessTypeToStringConverter res;
            return res;
        }

        std::string_view convert(AccessType type) const
        {
            return access_type_to_string_mapping[static_cast<size_t>(type)];
        }

    private:
        AccessTypeToStringConverter()
        {
#define ACCESS_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING(name, aliases, node_type, parent_group_name) \
            addToMapping(AccessType::name, #name);

            APPLY_FOR_ACCESS_TYPES(ACCESS_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING)

#undef ACCESS_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING
        }

        void addToMapping(AccessType type, const std::string_view & str)
        {
            String str2{str};
            boost::replace_all(str2, "_", " ");
            size_t index = static_cast<size_t>(type);
            access_type_to_string_mapping.resize(std::max(index + 1, access_type_to_string_mapping.size()));
            access_type_to_string_mapping[index] = str2;
        }

        Strings access_type_to_string_mapping;
    };
}

inline std::string_view toString(AccessType type) { return impl::AccessTypeToStringConverter<>::instance().convert(type); }

}
