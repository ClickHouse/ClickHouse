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
    M(SHOW_DATABASES, "", DATABASE, SHOW) /* allows to execute SHOW DATABASES, SHOW CREATE DATABASE, USE <database>*/\
    M(SHOW_TABLES, "", TABLE, SHOW) /* allows to execute SHOW TABLES, EXISTS <table>, CHECK <table> */\
    M(SHOW_COLUMNS, "", COLUMN, SHOW) /* allows to execute SHOW CREATE TABLE, DESCRIBE */\
    M(SHOW_DICTIONARIES, "", DICTIONARY, SHOW) /* allows to execute SHOW DICTIONARIES, SHOW CREATE DICTIONARY, EXISTS <dictionary> */\
    M(SHOW, "", GROUP, ALL) /* allows to execute SHOW, USE, EXISTS, CHECK, DESCRIBE */\
    \
    M(SELECT, "", COLUMN, ALL) \
    M(INSERT, "", COLUMN, ALL) \
    M(UPDATE, "ALTER UPDATE", COLUMN, ALTER_TABLE) /* allows to execute ALTER UPDATE */\
    M(DELETE, "ALTER DELETE", COLUMN, ALTER_TABLE) /* allows to execute ALTER DELETE */\
    \
    M(ADD_COLUMN, "ALTER ADD COLUMN", COLUMN, ALTER_COLUMN) \
    M(MODIFY_COLUMN, "ALTER MODIFY COLUMN", COLUMN, ALTER_COLUMN) \
    M(DROP_COLUMN, "ALTER DROP COLUMN", COLUMN, ALTER_COLUMN) \
    M(COMMENT_COLUMN, "ALTER COMMENT COLUMN", COLUMN, ALTER_COLUMN) \
    M(CLEAR_COLUMN, "ALTER CLEAR COLUMN", COLUMN, ALTER_COLUMN) \
    M(RENAME_COLUMN, "ALTER RENAME COLUMN", COLUMN, ALTER_COLUMN) \
    M(ALTER_COLUMN, "", GROUP, ALTER_TABLE) /* allow to execute ALTER {ADD|DROP|MODIFY...} COLUMN */\
    \
    M(ALTER_ORDER_BY, "MODIFY ORDER BY, ALTER MODIFY ORDER BY", TABLE, INDEX) \
    M(ADD_INDEX, "ALTER ADD INDEX", TABLE, INDEX) \
    M(DROP_INDEX, "ALTER DROP INDEX", TABLE, INDEX) \
    M(MATERIALIZE_INDEX, "ALTER MATERIALIZE INDEX", TABLE, INDEX) \
    M(CLEAR_INDEX, "ALTER CLEAR INDEX", TABLE, INDEX) \
    M(INDEX, "ALTER INDEX", GROUP, ALTER_TABLE) /* allows to execute ALTER ORDER BY or ALTER {ADD|DROP...} INDEX */\
    \
    M(ADD_CONSTRAINT, "ALTER ADD CONSTRAINT", TABLE, CONSTRAINT) \
    M(DROP_CONSTRAINT, "ALTER DROP CONSTRAINT", TABLE, CONSTRAINT) \
    M(CONSTRAINT, "ALTER CONSTRAINT", GROUP, ALTER_TABLE) /* allows to execute ALTER {ADD|DROP} CONSTRAINT */\
    \
    M(MODIFY_TTL, "ALTER MODIFY TTL", TABLE, ALTER_TABLE) /* allows to execute ALTER MODIFY TTL */\
    M(MATERIALIZE_TTL, "ALTER MATERIALIZE TTL", TABLE, ALTER_TABLE) /* allows to execute ALTER MATERIALIZE TTL */\
    M(MODIFY_SETTING, "ALTER MODIFY SETTING", TABLE, ALTER_TABLE) /* allows to execute ALTER MODIFY SETTING */\
    M(MOVE_PARTITION, "ALTER MOVE PARTITION, MOVE PART, ALTER MOVE PART", TABLE, ALTER_TABLE) \
    M(FETCH_PARTITION, "ALTER FETCH PARTITION", TABLE, ALTER_TABLE) \
    M(FREEZE_PARTITION, "ALTER FREEZE PARTITION", TABLE, ALTER_TABLE) \
    \
    M(ALTER_TABLE, "", GROUP, ALTER) \
    \
    M(REFRESH_VIEW, "ALTER LIVE VIEW REFRESH", VIEW, ALTER_VIEW) \
    M(MODIFY_VIEW_QUERY, "ALTER TABLE MODIFY QUERY", VIEW, ALTER_VIEW) \
    M(ALTER_VIEW, "", GROUP, ALTER) /* allows to execute ALTER LIVE VIEW REFRESH, ALTER TABLE MODIFY QUERY */\
    \
    M(ALTER, "", GROUP, ALL) /* allows to execute ALTER {TABLE|LIVE VIEW} */\
    \
    M(CREATE_DATABASE, "", DATABASE, CREATE) /* allows to execute {CREATE|ATTACH} DATABASE */\
    M(CREATE_TABLE, "", TABLE, CREATE) /* allows to execute {CREATE|ATTACH} {TABLE|VIEW} */\
    M(CREATE_VIEW, "", VIEW, CREATE) /* allows to execute {CREATE|ATTACH} VIEW */\
    M(CREATE_DICTIONARY, "", DICTIONARY, CREATE) /* allows to execute {CREATE|ATTACH} DICTIONARY */\
    M(CREATE_TEMPORARY_TABLE, "", GLOBAL, CREATE) /* allows to create and manipulate temporary tables */ \
    M(CREATE, "", GROUP, ALL) /* allows to execute {CREATE|ATTACH} */ \
    \
    M(DROP_DATABASE, "", DATABASE, DROP) /* allows to execute {DROP|DETACH} DATABASE */\
    M(DROP_TABLE, "", TABLE, DROP) /* allows to execute {DROP|DETACH} TABLE */\
    M(DROP_VIEW, "", VIEW, DROP) /* allows to execute {DROP|DETACH} TABLE for views */\
    M(DROP_DICTIONARY, "", DICTIONARY, DROP) /* allows to execute {DROP|DETACH} DICTIONARY */\
    M(DROP, "", GROUP, ALL) /* allows to execute {DROP|DETACH} */\
    \
    M(TRUNCATE, "TRUNCATE TABLE", TABLE, ALL) \
    M(OPTIMIZE, "OPTIMIZE TABLE", TABLE, ALL) \
    \
    M(KILL_QUERY, "", GLOBAL, ALL) /* allows to kill a query started by another user (anyone can kill his own queries) */\
    \
    M(CREATE_USER, "", GLOBAL, ALL) \
    M(ALTER_USER, "", GLOBAL, ALL) \
    M(DROP_USER, "", GLOBAL, ALL) \
    M(CREATE_ROLE, "", GLOBAL, ALL) \
    M(ALTER_ROLE, "", GLOBAL, ALL) \
    M(DROP_ROLE, "", GLOBAL, ALL) \
    M(ROLE_ADMIN, "", GLOBAL, ALL) /* allows to grant and revoke the roles which are not granted to the current user with admin option */\
    M(CREATE_ROW_POLICY, "CREATE POLICY", GLOBAL, ALL) \
    M(ALTER_ROW_POLICY, "ALTER POLICY", GLOBAL, ALL) \
    M(DROP_ROW_POLICY, "DROP POLICY", GLOBAL, ALL) \
    M(CREATE_QUOTA, "", GLOBAL, ALL) \
    M(ALTER_QUOTA, "", GLOBAL, ALL) \
    M(DROP_QUOTA, "", GLOBAL, ALL) \
    M(CREATE_SETTINGS_PROFILE, "CREATE PROFILE", GLOBAL, ALL) \
    M(ALTER_SETTINGS_PROFILE, "ALTER PROFILE", GLOBAL, ALL) \
    M(DROP_SETTINGS_PROFILE, "DROP PROFILE", GLOBAL, ALL) \
    \
    M(SHUTDOWN, "SYSTEM SHUTDOWN, SYSTEM KILL", GLOBAL, SYSTEM) \
    M(DROP_CACHE, "SYSTEM DROP CACHE, DROP DNS CACHE, SYSTEM DROP DNS CACHE, DROP MARK CACHE, SYSTEM DROP MARK CACHE, DROP UNCOMPRESSED CACHE, SYSTEM DROP UNCOMPRESSED CACHE, DROP COMPILED EXPRESSION CACHE, SYSTEM DROP COMPILED EXPRESSION CACHE", GLOBAL, SYSTEM) \
    M(RELOAD_CONFIG, "SYSTEM RELOAD CONFIG", GLOBAL, SYSTEM) \
    M(RELOAD_DICTIONARY, "SYSTEM RELOAD DICTIONARY, RELOAD DICTIONARIES, SYSTEM RELOAD DICTIONARIES, RELOAD EMBEDDED DICTIONARIES, SYSTEM RELOAD EMBEDDED DICTIONARIES", GLOBAL, SYSTEM) \
    M(STOP_MERGES, "SYSTEM STOP MERGES, START MERGES, SYSTEM START MERGES", TABLE, SYSTEM) \
    M(STOP_TTL_MERGES, "SYSTEM STOP TTL MERGES, START TTL MERGES, SYSTEM START TTL MERGES", TABLE, SYSTEM) \
    M(STOP_FETCHES, "SYSTEM STOP FETCHES, START FETCHES, SYSTEM START FETCHES", TABLE, SYSTEM) \
    M(STOP_MOVES, "SYSTEM STOP MOVES, START MOVES, SYSTEM START MOVES", TABLE, SYSTEM) \
    M(STOP_DISTRIBUTED_SENDS, "SYSTEM STOP DISTRIBUTED SENDS, START DISTRIBUTED SENDS, SYSTEM START DISTRIBUTED SENDS", TABLE, SYSTEM) \
    M(STOP_REPLICATED_SENDS, "SYSTEM STOP REPLICATED SENDS, START REPLICATED SENDS, SYSTEM START REPLICATED SENDS", TABLE, SYSTEM) \
    M(STOP_REPLICATION_QUEUES, "SYSTEM STOP REPLICATION QUEUES, START REPLICATION QUEUES, SYSTEM START REPLICATION QUEUES", TABLE, SYSTEM) \
    M(SYNC_REPLICA, "SYSTEM SYNC REPLICA", TABLE, SYSTEM) \
    M(RESTART_REPLICA, "SYSTEM RESTART REPLICA", TABLE, SYSTEM) \
    M(FLUSH_DISTRIBUTED, "SYSTEM FLUSH DISTRIBUTED", TABLE, SYSTEM) \
    M(FLUSH_LOGS, "SYSTEM FLUSH LOGS", GLOBAL, SYSTEM) \
    M(SYSTEM, "", GROUP, ALL) /* allows to execute SYSTEM {SHUTDOWN|RELOAD CONFIG|...} */ \
    \
    M(dictGet, "dictHas, dictGetHierarchy, dictIsIn", DICTIONARY, ALL) /* allows to execute functions dictGet(), dictHas(), dictGetHierarchy(), dictIsIn() */\
    \
    M(addressToLine, "", GLOBAL, INTROSPECTION) /* allows to execute function addressToLine() */\
    M(addressToSymbol, "", GLOBAL, INTROSPECTION) /* allows to execute function addressToSymbol() */\
    M(demangle, "", GLOBAL, INTROSPECTION) /* allows to execute function demangle() */\
    M(INTROSPECTION, "INTROSPECTION FUNCTIONS", GROUP, ALL) /* allows to execute functions addressToLine(), addressToSymbol(), demangle()*/\
    \
    M(file, "", GLOBAL, TABLE_FUNCTIONS) \
    M(url, "", GLOBAL, TABLE_FUNCTIONS) \
    M(input, "", GLOBAL, TABLE_FUNCTIONS) \
    M(values, "", GLOBAL, TABLE_FUNCTIONS) \
    M(numbers, "", GLOBAL, TABLE_FUNCTIONS) \
    M(zeros, "", GLOBAL, TABLE_FUNCTIONS) \
    M(merge, "", GLOBAL, TABLE_FUNCTIONS) \
    M(remote, "remoteSecure, cluster", GLOBAL, TABLE_FUNCTIONS) \
    M(mysql, "", GLOBAL, TABLE_FUNCTIONS) \
    M(odbc, "", GLOBAL, TABLE_FUNCTIONS) \
    M(jdbc, "", GLOBAL, TABLE_FUNCTIONS) \
    M(hdfs, "", GLOBAL, TABLE_FUNCTIONS) \
    M(s3, "", GLOBAL, TABLE_FUNCTIONS) \
    M(TABLE_FUNCTIONS, "", GROUP, ALL) \
    \
    M(ALL, "ALL PRIVILEGES", GROUP, NONE) /* full access */ \
    M(NONE, "USAGE, NO PRIVILEGES", GROUP, NONE) /* no access */

#define DECLARE_ACCESS_TYPE_ENUM_CONST(name, aliases, node_type, parent_group_name) \
    name,

    APPLY_FOR_ACCESS_TYPES(DECLARE_ACCESS_TYPE_ENUM_CONST)
#undef DECLARE_ACCESS_TYPE_ENUM_CONST
};

constexpr size_t MAX_ACCESS_TYPE = static_cast<size_t>(AccessType::TABLE_FUNCTIONS) + 1;

std::string_view toString(AccessType type);


namespace impl
{
    template <typename = void>
    class AccessTypeToKeywordConverter
    {
    public:
        static const AccessTypeToKeywordConverter & instance()
        {
            static const AccessTypeToKeywordConverter res;
            return res;
        }

        std::string_view convert(AccessType type) const
        {
            return access_type_to_keyword_mapping[static_cast<size_t>(type)];
        }

    private:
        AccessTypeToKeywordConverter()
        {
#define INSERT_ACCESS_TYPE_KEYWORD_PAIR_TO_MAPPING(name, aliases, node_type, parent_group_name) \
            insertToMapping(AccessType::name, #name);

            APPLY_FOR_ACCESS_TYPES(INSERT_ACCESS_TYPE_KEYWORD_PAIR_TO_MAPPING)

#undef INSERT_ACCESS_TYPE_KEYWORD_PAIR_TO_MAPPING
        }

        void insertToMapping(AccessType type, const std::string_view & str)
        {
            String str2{str};
            boost::replace_all(str2, "_", " ");
            size_t index = static_cast<size_t>(type);
            access_type_to_keyword_mapping.resize(std::max(index + 1, access_type_to_keyword_mapping.size()));
            access_type_to_keyword_mapping[index] = str2;
        }

        Strings access_type_to_keyword_mapping;
    };
}

inline std::string_view toKeyword(AccessType type) { return impl::AccessTypeToKeywordConverter<>::instance().convert(type); }

}
