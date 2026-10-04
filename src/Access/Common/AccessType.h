#pragma once

#include <Common/StringUtils.h>
#include <Common/Exception.h>
#include <base/types.h>

#include <map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// This namespace defines objects which can be used as a parameter in global_with_parameter access types.
/// Each such object should be an enum and have a APPLY_FOR_X(M) macro.
/// Macro M should be defined as M(name, aliases).
namespace AccessTypeObjects
{

/// Represents parameter for the SOURCE access type level. Uses corresponding table engine names as an alias.
enum class Source : uint8_t
{
#define APPLY_FOR_SOURCE(M) \
    M(FILE, "File") \
    M(URL, "") \
    M(REMOTE, "Distributed") \
    M(MONGO, "MongoDB") \
    M(REDIS, "Redis") \
    M(MYSQL, "MySQL") \
    M(POSTGRES, "PostgreSQL") \
    M(SQLITE, "SQLite") \
    M(ODBC, "") \
    M(JDBC, "") \
    M(HDFS, "") \
    M(S3, "") \
    M(HIVE, "Hive") \
    M(AZURE, "AzureBlobStorage") \
    M(KAFKA, "Kafka") \
    M(NATS, "") \
    M(RABBITMQ, "RabbitMQ") \
    M(YTSAURUS, "YTsaurus") \
    M(ARROW_FLIGHT, "ArrowFlight") \
    M(BIGQUERY, "BigQuery") \
    M(DISK, "Disk") \

#define DECLARE_ACCESS_TYPE_OBJECTS_ENUM_CONST(name, aliases) name,

    APPLY_FOR_SOURCE(DECLARE_ACCESS_TYPE_OBJECTS_ENUM_CONST)
#undef DECLARE_ACCESS_TYPE_OBJECTS_ENUM_CONST
};


#define ACCESS_TYPE_OBJECT_ADD_TO_MAPPING(name, aliases) \
        addToMapping(Source::name, #name); \
        addAliases(Source::name, aliases);

#define ENUM_ACCESS_OBJECT(NAME, M) \
class EnumHolder##NAME \
{ \
public: \
    static const EnumHolder##NAME & instance() \
    { \
        static const EnumHolder##NAME res; \
        return res; \
    } \
    \
    std::string_view toString(NAME type) const { return entity_enum_to_string[static_cast<size_t>(type)]; } \
    \
    NAME fromString(const String & name) const \
    { \
        if (auto it = aliases.find(name); it != aliases.end()) \
            return it->second; \
        \
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unable to find AccessTypeObject {} by name {}", #NAME, name); \
    } \
    \
    bool validate(const String & name) const { return aliases.contains(name); } \
    \
    const std::map<String, NAME> & getAliasesMap() const { return aliases; } \
    \
private: \
    EnumHolder##NAME() \
    { \
        M(ACCESS_TYPE_OBJECT_ADD_TO_MAPPING) \
    } \
    \
    void addToMapping(NAME type, std::string_view str) \
    { \
        String str2{str}; \
        size_t index = static_cast<size_t>(type); \
        entity_enum_to_string.resize(std::max(index + 1, entity_enum_to_string. size())); \
        entity_enum_to_string[index] = str2; \
    } \
    \
    void addAliases(NAME type, std::string_view str) \
    { \
        String str2{str}; \
        std::vector<String> type_aliases; \
        \
        for (size_t begin = 0; begin <= str2.length();) \
        { \
            size_t end = str2.find(',', begin); \
            if (end == String::npos) \
                end = str2.length(); \
            type_aliases.push_back(trim(str2.substr(begin, end - begin), isWhitespaceASCII)); \
            begin = end + 1; \
        } \
        for (auto & alias : type_aliases) \
            aliases[alias] = type; \
        \
        aliases[String{toString(type)}] = type; \
    } \
    \
    std::vector<String> entity_enum_to_string; \
    std::map<String, NAME> aliases; \
}; \
\
inline auto toString##NAME(NAME type) \
{ \
    return EnumHolder##NAME::instance().toString(type); \
} \
\
inline auto fromString##NAME(const String & name) \
{ \
    return EnumHolder##NAME::instance().fromString(name); \
} \
inline auto unify##NAME(const String & name) \
{ \
    return toString##NAME(fromString##NAME(name)); \
} \
\
inline auto & getAliasesMap##NAME() \
{ \
    return EnumHolder##NAME::instance().getAliasesMap(); \
} \
\
inline auto validate##NAME(const String & name) \
{ \
    return EnumHolder##NAME::instance().validate(name); \
}

ENUM_ACCESS_OBJECT(Source, APPLY_FOR_SOURCE)

#undef ENUM_ACCESS_OBJECT
#undef ACCESS_TYPE_OBJECT_ADD_TO_MAPPING
}


#define M_OBSOLETE(M, name, aliases, node_type, parent_group_name) \
    M(name, aliases, node_type, parent_group_name, true)


/// Represents an access type which can be granted on databases, tables, columns, etc.
enum class AccessType : uint8_t
{
/// Macro M should be defined as M(name, aliases, node_type, parent_group_name, is_obsolete)
/// where name is identifier with underscores (instead of spaces);
/// aliases is a string containing comma-separated list;
/// node_type either specifies access type's level (GLOBAL/NAMED_COLLECTION/USER_NAME/SOURCE/DATABASE/TABLE/DICTIONARY/VIEW/COLUMNS),
/// or specifies that the access type is a GROUP of other access types;
/// parent_group_name is the name of the group containing this access type (or NONE if there is no such group);
/// is_obsolete is `true` for obsolete access types, which are declared with `M_OBSOLETE`.
/// NOTE A parent group must be declared AFTER all its children.
#define APPLY_FOR_ACCESS_TYPES(M) \
    M(SHOW_DATABASES, "", DATABASE, SHOW, false) /* allows to execute SHOW DATABASES, SHOW CREATE DATABASE, USE <database>;
                                             implicitly enabled by any grant on the database */\
    M(SHOW_TABLES, "", TABLE, SHOW, false) /* allows to execute SHOW TABLES, EXISTS <table>;
                                       implicitly enabled by any grant on the table */\
    M(CHECK, "", TABLE, ALL, false) /* allows to execute CHECK TABLE; */\
    M(SHOW_COLUMNS, "", COLUMN, SHOW, false) /* allows to execute SHOW CREATE TABLE, DESCRIBE;
                                         implicitly enabled with any grant on the column */\
    M(SHOW_DICTIONARIES, "", DICTIONARY, SHOW, false) /* allows to execute SHOW DICTIONARIES, SHOW CREATE DICTIONARY, EXISTS <dictionary>;
                                                  implicitly enabled by any grant on the dictionary */\
    M(SHOW, "", GROUP, ALL, false) /* allows to execute SHOW, USE, EXISTS, DESCRIBE */\
    M(SHOW_FILESYSTEM_CACHES, "", GROUP, ALL, false) \
    \
    M(SELECT, "", COLUMN, ALL, false) \
    M(INSERT, "", COLUMN, ALL, false) \
    M(ALTER_UPDATE, "UPDATE", COLUMN, ALTER_TABLE, false) /* allows to execute ALTER UPDATE */\
    M(ALTER_DELETE, "DELETE", COLUMN, ALTER_TABLE, false) /* allows to execute ALTER DELETE */\
    \
    M(ALTER_ADD_COLUMN, "ADD COLUMN", COLUMN, ALTER_COLUMN, false) \
    M(ALTER_MODIFY_COLUMN, "MODIFY COLUMN", COLUMN, ALTER_COLUMN, false) \
    M(ALTER_DROP_COLUMN, "DROP COLUMN", COLUMN, ALTER_COLUMN, false) \
    M(ALTER_COMMENT_COLUMN, "COMMENT COLUMN", COLUMN, ALTER_COLUMN, false) \
    M(ALTER_CLEAR_COLUMN, "CLEAR COLUMN", COLUMN, ALTER_COLUMN, false) \
    M(ALTER_RENAME_COLUMN, "RENAME COLUMN", COLUMN, ALTER_COLUMN, false) \
    M(ALTER_MATERIALIZE_COLUMN, "MATERIALIZE COLUMN", COLUMN, ALTER_COLUMN, false) \
    M(ALTER_COLUMN, "", GROUP, ALTER_TABLE, false) /* allow to execute ALTER {ADD|DROP|MODIFY...} COLUMN */\
    M(ALTER_MODIFY_COMMENT, "MODIFY COMMENT", TABLE, ALTER_TABLE, false) /* modify table comment */\
    M(ALTER_MODIFY_DATABASE_COMMENT, "MODIFY DATABASE COMMENT", DATABASE, ALTER_DATABASE, false) /* modify database comment*/\
    \
    M(ALTER_ORDER_BY, "ALTER MODIFY ORDER BY, MODIFY ORDER BY", TABLE, ALTER_INDEX, false) \
    M(ALTER_SAMPLE_BY, "ALTER MODIFY SAMPLE BY, MODIFY SAMPLE BY", TABLE, ALTER_INDEX, false) \
    M(ALTER_ADD_INDEX, "ADD INDEX", TABLE, ALTER_INDEX, false) \
    M(ALTER_DROP_INDEX, "DROP INDEX", TABLE, ALTER_INDEX, false) \
    M(ALTER_MATERIALIZE_INDEX, "MATERIALIZE INDEX", TABLE, ALTER_INDEX, false) \
    M(ALTER_CLEAR_INDEX, "CLEAR INDEX", TABLE, ALTER_INDEX, false) \
    M(ALTER_INDEX, "INDEX", GROUP, ALTER_TABLE, false) /* allows to execute ALTER ORDER BY or ALTER {ADD|DROP...} INDEX */\
    \
    M(ALTER_ADD_STATISTICS, "ALTER ADD STATISTIC", TABLE, ALTER_STATISTICS, false) \
    M(ALTER_DROP_STATISTICS, "ALTER DROP STATISTIC", TABLE, ALTER_STATISTICS, false) \
    M(ALTER_MODIFY_STATISTICS, "ALTER MODIFY STATISTIC", TABLE, ALTER_STATISTICS, false) \
    M(ALTER_MATERIALIZE_STATISTICS, "ALTER MATERIALIZE STATISTIC", TABLE, ALTER_STATISTICS, false) \
    M(ALTER_STATISTICS, "STATISTIC", GROUP, ALTER_TABLE, false) /* allows to execute ALTER STATISTIC */\
    \
    M(ALTER_ADD_PROJECTION, "ADD PROJECTION", TABLE, ALTER_PROJECTION, false) \
    M(ALTER_DROP_PROJECTION, "DROP PROJECTION", TABLE, ALTER_PROJECTION, false) \
    M(ALTER_MODIFY_PROJECTION, "MODIFY PROJECTION", TABLE, ALTER_PROJECTION, false) \
    M(ALTER_MATERIALIZE_PROJECTION, "MATERIALIZE PROJECTION", TABLE, ALTER_PROJECTION, false) \
    M(ALTER_CLEAR_PROJECTION, "CLEAR PROJECTION", TABLE, ALTER_PROJECTION, false) \
    M(ALTER_PROJECTION, "PROJECTION", GROUP, ALTER_TABLE, false) /* allows to execute ALTER ORDER BY or ALTER {ADD|DROP...} PROJECTION */\
    \
    M(ALTER_ADD_CONSTRAINT, "ADD CONSTRAINT", TABLE, ALTER_CONSTRAINT, false) \
    M(ALTER_DROP_CONSTRAINT, "DROP CONSTRAINT", TABLE, ALTER_CONSTRAINT, false) \
    M(ALTER_MODIFY_CONSTRAINT, "MODIFY CONSTRAINT", TABLE, ALTER_CONSTRAINT, false) \
    M(ALTER_CONSTRAINT, "CONSTRAINT", GROUP, ALTER_TABLE, false) /* allows to execute ALTER {ADD|DROP|MODIFY} CONSTRAINT */\
    \
    M(ALTER_TTL, "ALTER MODIFY TTL, MODIFY TTL", TABLE, ALTER_TABLE, false) /* allows to execute ALTER MODIFY TTL */\
    M(ALTER_MATERIALIZE_TTL, "MATERIALIZE TTL", TABLE, ALTER_TABLE, false) /* allows to execute ALTER MATERIALIZE TTL;
                                                                       enabled implicitly by the grant ALTER_TABLE */\
    M(ALTER_REWRITE_PARTS, "REWRITE PARTS", TABLE, ALTER_TABLE, false) /* allows to execute ALTER REWRITE PARTS */\
    M(ALTER_SETTINGS, "ALTER SETTING, ALTER MODIFY SETTING, MODIFY SETTING, RESET SETTING", TABLE, ALTER_TABLE, false) /* allows to execute ALTER MODIFY SETTING */\
    M(ALTER_MOVE_PARTITION, "ALTER MOVE PART, MOVE PARTITION, MOVE PART", TABLE, ALTER_TABLE, false) \
    M(ALTER_FETCH_PARTITION, "ALTER FETCH PART, FETCH PARTITION", TABLE, ALTER_TABLE, false) \
    M(ALTER_FREEZE_PARTITION, "FREEZE PARTITION, UNFREEZE", TABLE, ALTER_TABLE, false) \
    M(ALTER_UNLOCK_SNAPSHOT, "UNLOCK SNAPSHOT", TABLE, ALTER_TABLE, false) \
    M(ALTER_EXECUTE, "ALTER TABLE EXECUTE", TABLE, ALTER_TABLE, false) \
    \
    M(ALTER_DATABASE_SETTINGS, "ALTER DATABASE SETTING, ALTER MODIFY DATABASE SETTING, MODIFY DATABASE SETTING", DATABASE, ALTER_DATABASE, false) /* allows to execute ALTER MODIFY SETTING */\
    M(ALTER_NAMED_COLLECTION, "", NAMED_COLLECTION, NAMED_COLLECTION_ADMIN, false) /* allows to execute ALTER NAMED COLLECTION */\
    M(ALTER_HANDLER, "", GLOBAL, ALTER, false) /* allows to execute ALTER HANDLER */\
    \
    M(ALTER_TABLE, "", GROUP, ALTER, false) \
    M(ALTER_DATABASE, "", GROUP, ALTER, false) \
    \
    M(ALTER_VIEW_MODIFY_QUERY, "ALTER TABLE MODIFY QUERY", VIEW, ALTER_VIEW, false) \
    M(ALTER_VIEW_MODIFY_REFRESH, "ALTER TABLE MODIFY QUERY", VIEW, ALTER_VIEW, false) \
    M(ALTER_VIEW_MODIFY_SQL_SECURITY, "ALTER TABLE MODIFY SQL SECURITY", VIEW, ALTER_VIEW, false) \
    M(ALTER_VIEW, "", GROUP, ALTER, false) /* allows to execute ALTER VIEW REFRESH, ALTER VIEW MODIFY QUERY, ALTER VIEW MODIFY REFRESH;
                                       implicitly enabled by the grant ALTER_TABLE */\
    \
    M(ALTER, "", GROUP, ALL, false) /* allows to execute ALTER TABLE */\
    \
    M(CREATE_DATABASE, "", DATABASE, CREATE, false) /* allows to execute {CREATE|ATTACH} DATABASE */\
    M(CREATE_TABLE, "", TABLE, CREATE, false) /* allows to execute {CREATE|ATTACH} {TABLE|VIEW} */\
    M(CREATE_VIEW, "", VIEW, CREATE, false) /* allows to execute {CREATE|ATTACH} VIEW;
                                        implicitly enabled by the grant CREATE_TABLE */\
    M(CREATE_DICTIONARY, "", DICTIONARY, CREATE, false) /* allows to execute {CREATE|ATTACH} DICTIONARY */\
    M(CREATE_TEMPORARY_TABLE, "", GLOBAL, CREATE_ARBITRARY_TEMPORARY_TABLE, false) /* allows to create and manipulate temporary tables;
                                                     implicitly enabled by the grant CREATE_TABLE on any table */ \
    M(CREATE_ARBITRARY_TEMPORARY_TABLE, "", GLOBAL, CREATE, false)  /* allows to create  and manipulate temporary tables
                                                                with arbitrary table engine */\
    M(CREATE_TEMPORARY_VIEW, "", GLOBAL, CREATE, false) /* allows to create and manipulate temporary tables;
                                                     implicitly enabled by the grant CREATE_VIEW on any table */ \
    M(CREATE_FUNCTION, "", GLOBAL, CREATE, false) /* allows to execute CREATE FUNCTION */ \
    M(CREATE_WORKLOAD, "", GLOBAL, CREATE, false) /* allows to execute CREATE WORKLOAD */ \
    M(CREATE_RESOURCE, "", GLOBAL, CREATE, false) /* allows to execute CREATE RESOURCE */ \
    M(CREATE_NAMED_COLLECTION, "", NAMED_COLLECTION, NAMED_COLLECTION_ADMIN, false) /* allows to execute CREATE NAMED COLLECTION */ \
    M(CREATE_HANDLER, "", GLOBAL, CREATE, false) /* allows to execute CREATE HANDLER */ \
    M(CREATE, "", GROUP, ALL, false) /* allows to execute {CREATE|ATTACH} */ \
    \
    M(DROP_DATABASE, "", DATABASE, DROP, false) /* allows to execute {DROP|DETACH|TRUNCATE} DATABASE */\
    M(DROP_TABLE, "", TABLE, DROP, false) /* allows to execute {DROP|DETACH} TABLE */\
    M(DROP_VIEW, "", VIEW, DROP, false) /* allows to execute {DROP|DETACH} TABLE for views;
                                    implicitly enabled by the grant DROP_TABLE */\
    M(DROP_DICTIONARY, "", DICTIONARY, DROP, false) /* allows to execute {DROP|DETACH} DICTIONARY */\
    M(DROP_FUNCTION, "", GLOBAL, DROP, false) /* allows to execute DROP FUNCTION */\
    M(DROP_WORKLOAD, "", GLOBAL, DROP, false) /* allows to execute DROP WORKLOAD */\
    M(DROP_RESOURCE, "", GLOBAL, DROP, false) /* allows to execute DROP RESOURCE */\
    M(DROP_NAMED_COLLECTION, "", NAMED_COLLECTION, NAMED_COLLECTION_ADMIN, false) /* allows to execute DROP NAMED COLLECTION */\
    M(DROP_HANDLER, "", GLOBAL, DROP, false) /* allows to execute DROP HANDLER */\
    M(DROP, "", GROUP, ALL, false) /* allows to execute {DROP|DETACH} */\
    \
    M(UNDROP_TABLE, "", TABLE, ALL, false) /* allows to execute {UNDROP} TABLE */\
    \
    M(TRUNCATE, "TRUNCATE TABLE", TABLE, ALL, false) \
    M(OPTIMIZE, "OPTIMIZE TABLE", TABLE, ALL, false) \
    M(BACKUP, "", TABLE, ALL, false) /* allows to backup tables */\
    \
    M(KILL_QUERY, "", GLOBAL, ALL, false) /* allows to kill a query started by another user
                                      (anyone can kill his own queries) */\
    M(KILL_TRANSACTION, "", GLOBAL, ALL, false) \
    \
    M(MOVE_PARTITION_BETWEEN_SHARDS, "", GLOBAL, ALL, false) /* required to be able to move a part/partition to a table
                                                         identified by its ZooKeeper path */\
    \
    M(CREATE_USER, "", USER_NAME, ACCESS_MANAGEMENT, false) \
    M(ALTER_USER, "", USER_NAME, ACCESS_MANAGEMENT, false) \
    M(DROP_USER, "", USER_NAME, ACCESS_MANAGEMENT, false) \
    M(CREATE_TOKEN, "", GLOBAL, ACCESS_MANAGEMENT, false) /* allows to add an authentication method to your own user,
                                                      with `CREATE TOKEN` or `ALTER USER <current user> ADD IDENTIFIED` */\
    M(CREATE_ROLE, "", USER_NAME, ACCESS_MANAGEMENT, false) \
    M(ALTER_ROLE, "", USER_NAME, ACCESS_MANAGEMENT, false) \
    M(DROP_ROLE, "", USER_NAME, ACCESS_MANAGEMENT, false) \
    M(ROLE_ADMIN, "", GLOBAL, ACCESS_MANAGEMENT, false) /* allows to grant and revoke the roles which are not granted to the current user with admin option */\
    M(CREATE_ROW_POLICY, "CREATE POLICY", TABLE, ACCESS_MANAGEMENT, false) \
    M(ALTER_ROW_POLICY, "ALTER POLICY", TABLE, ACCESS_MANAGEMENT, false) \
    M(DROP_ROW_POLICY, "DROP POLICY", TABLE, ACCESS_MANAGEMENT, false) \
    M(CREATE_MASKING_POLICY, "", GLOBAL, ACCESS_MANAGEMENT, false) \
    M(ALTER_MASKING_POLICY, "", GLOBAL, ACCESS_MANAGEMENT, false) \
    M(DROP_MASKING_POLICY, "", GLOBAL, ACCESS_MANAGEMENT, false) \
    M(CREATE_QUOTA, "", GLOBAL, ACCESS_MANAGEMENT, false) \
    M(ALTER_QUOTA, "", GLOBAL, ACCESS_MANAGEMENT, false) \
    M(DROP_QUOTA, "", GLOBAL, ACCESS_MANAGEMENT, false) \
    M(CREATE_SETTINGS_PROFILE, "CREATE PROFILE", GLOBAL, ACCESS_MANAGEMENT, false) \
    M(ALTER_SETTINGS_PROFILE, "ALTER PROFILE", GLOBAL, ACCESS_MANAGEMENT, false) \
    M(DROP_SETTINGS_PROFILE, "DROP PROFILE", GLOBAL, ACCESS_MANAGEMENT, false) \
    M(ALLOW_SQL_SECURITY_NONE, "CREATE SQL SECURITY NONE, ALLOW SQL SECURITY NONE, SQL SECURITY NONE, SECURITY NONE", GLOBAL, ACCESS_MANAGEMENT, false) \
    M(SHOW_USERS, "SHOW CREATE USER", GLOBAL, SHOW_ACCESS, false) \
    M(SHOW_ROLES, "SHOW CREATE ROLE", GLOBAL, SHOW_ACCESS, false) \
    M(SHOW_ROW_POLICIES, "SHOW POLICIES, SHOW CREATE ROW POLICY, SHOW CREATE POLICY", TABLE, SHOW_ACCESS, false) \
    M(SHOW_QUOTAS, "SHOW CREATE QUOTA", GLOBAL, SHOW_ACCESS, false) \
    M(SHOW_SETTINGS_PROFILES, "SHOW PROFILES, SHOW CREATE SETTINGS PROFILE, SHOW CREATE PROFILE", GLOBAL, SHOW_ACCESS, false) \
    M(SHOW_MASKING_POLICIES, "SHOW CREATE MASKING POLICY", GLOBAL, SHOW_ACCESS, false) \
    M(SHOW_ACCESS, "", GROUP, ACCESS_MANAGEMENT, false) \
    M(IMPERSONATE, "EXECUTE AS", USER_NAME, ACCESS_MANAGEMENT, false) \
    M(ACCESS_MANAGEMENT, "", GROUP, ALL, false) \
    M(SHOW_NAMED_COLLECTIONS, "SHOW NAMED COLLECTIONS", NAMED_COLLECTION, NAMED_COLLECTION_ADMIN, false) \
    M(SHOW_NAMED_COLLECTIONS_SECRETS, "SHOW NAMED COLLECTIONS SECRETS", NAMED_COLLECTION, NAMED_COLLECTION_ADMIN, false) \
    M(NAMED_COLLECTION, "NAMED COLLECTION USAGE, USE NAMED COLLECTION", NAMED_COLLECTION, NAMED_COLLECTION_ADMIN, false) \
    M(NAMED_COLLECTION_ADMIN, "NAMED COLLECTION CONTROL", NAMED_COLLECTION, ALL, false) \
    M(SHOW_HANDLERS, "SHOW HANDLER", GLOBAL, ALL, false) /* allows to see SQL-defined HTTP handlers in system.handlers */\
    M(SET_DEFINER, "", DEFINER, ALL, false) \
    \
    M(TABLE_ENGINE, "TABLE ENGINE", TABLE_ENGINE, ALL, false) \
    \
    M(SYSTEM_SHUTDOWN, "SYSTEM KILL, SHUTDOWN", GLOBAL, SYSTEM, false) \
    M(SYSTEM_DROP_DNS_CACHE, "SYSTEM CLEAR DNS CACHE, SYSTEM DROP DNS, DROP DNS CACHE, DROP DNS", GLOBAL, SYSTEM_DROP_CACHE, false)  \
    M(SYSTEM_DROP_CONNECTIONS_CACHE, "SYSTEM CLEAR CONNECTIONS CACHE, SYSTEM DROP CONNECTIONS CACHE, DROP CONNECTIONS CACHE", GLOBAL, SYSTEM_DROP_CACHE, false)  \
    M(SYSTEM_PREWARM_MARK_CACHE, "SYSTEM PREWARM MARK, PREWARM MARK CACHE, PREWARM MARKS", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_MARK_CACHE, "SYSTEM CLEAR MARK CACHE, SYSTEM DROP MARK, DROP MARK CACHE, DROP MARKS", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_ICEBERG_METADATA_CACHE, "SYSTEM CLEAR ICEBERG_METADATA_CACHE, SYSTEM DROP ICEBERG_METADATA_CACHE", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_PAIMON_METADATA_CACHE, "SYSTEM CLEAR PAIMON_METADATA_CACHE, SYSTEM DROP PAIMON_METADATA_CACHE", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_AVRO_SCHEMA_CACHE, "SYSTEM CLEAR AVRO SCHEMA CACHE, SYSTEM DROP AVRO SCHEMA CACHE, DROP AVRO SCHEMA CACHE", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_PARQUET_METADATA_CACHE, "SYSTEM DROP PARQUET_METADATA_CACHE", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_POINT_IN_POLYGON_CACHE, "SYSTEM CLEAR POINT IN POLYGON CACHE, SYSTEM DROP POINT IN POLYGON CACHE, DROP POINT IN POLYGON CACHE", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_PREWARM_PRIMARY_INDEX_CACHE, "SYSTEM PREWARM PRIMARY INDEX, PREWARM PRIMARY INDEX CACHE, PREWARM PRIMARY INDEX", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_PRIMARY_INDEX_CACHE, "SYSTEM CLEAR PRIMARY INDEX CACHE, SYSTEM DROP PRIMARY INDEX, DROP PRIMARY INDEX CACHE, DROP PRIMARY INDEX", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_UNCOMPRESSED_CACHE, "SYSTEM CLEAR UNCOMPRESSED CACHE, SYSTEM DROP UNCOMPRESSED, DROP UNCOMPRESSED CACHE, DROP UNCOMPRESSED", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_VECTOR_SIMILARITY_INDEX_CACHE, "SYSTEM CLEAR VECTOR SIMILARITY INDEX CACHE, SYSTEM DROP VECTOR SIMILARITY INDEX CACHE, DROP VECTOR SIMILARITY INDEX CACHE", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_TEXT_INDEX_TOKENS_CACHE, "SYSTEM CLEAR TEXT INDEX TOKENS CACHE, SYSTEM DROP TEXT INDEX TOKENS CACHE, DROP TEXT INDEX TOKENS CACHE", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_TEXT_INDEX_HEADER_CACHE, "SYSTEM CLEAR TEXT INDEX HEADER CACHE, SYSTEM DROP TEXT INDEX HEADER CACHE, DROP TEXT INDEX HEADER CACHE", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_TEXT_INDEX_POSTINGS_CACHE, "SYSTEM CLEAR TEXT INDEX POSTINGS CACHE, SYSTEM DROP TEXT INDEX POSTINGS CACHE, DROP TEXT INDEX POSTINGS CACHE", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_TEXT_INDEX_CACHES, "SYSTEM CLEAR TEXT INDEX CACHES, SYSTEM DROP TEXT INDEX CACHES, DROP TEXT INDEX CACHES", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_MMAP_CACHE, "SYSTEM CLEAR MMAP CACHE, SYSTEM DROP MMAP, DROP MMAP CACHE, DROP MMAP", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_QUERY_CONDITION_CACHE, "SYSTEM CLEAR QUERY CONDITION CACHE, SYSTEM DROP QUERY CONDITION, DROP QUERY CONDITION CACHE, DROP QUERY CONDITION", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_ENCRYPTION_HEADERS_CACHE, "SYSTEM CLEAR ENCRYPTION HEADERS CACHE, SYSTEM DROP ENCRYPTION HEADERS, DROP ENCRYPTION HEADERS CACHE, DROP ENCRYPTION HEADERS", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_QUERY_CACHE, "SYSTEM CLEAR QUERY CACHE, SYSTEM DROP QUERY, DROP QUERY CACHE, DROP QUERY", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_COMPILED_EXPRESSION_CACHE, "SYSTEM CLEAR COMPILED EXPRESSION CACHE, SYSTEM DROP COMPILED EXPRESSION, DROP COMPILED EXPRESSION CACHE, DROP COMPILED EXPRESSIONS", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_FILESYSTEM_CACHE, "SYSTEM CLEAR FILESYSTEM CACHE, SYSTEM DROP FILESYSTEM CACHE, DROP FILESYSTEM CACHE", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_DISTRIBUTED_CACHE, "SYSTEM CLEAR DISTRIBUTED CACHE, SYSTEM DROP DISTRIBUTED CACHE, DROP DISTRIBUTED CACHE", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_SYNC_FILESYSTEM_CACHE, "SYSTEM REPAIR FILESYSTEM CACHE, REPAIR FILESYSTEM CACHE, SYNC FILESYSTEM CACHE", GLOBAL, SYSTEM, false) \
    M(SYSTEM_DROP_PAGE_CACHE, "SYSTEM CLEAR PAGE CACHE, SYSTEM DROP PAGE CACHE, DROP PAGE CACHE", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_SCHEMA_CACHE, "SYSTEM CLEAR SCHEMA CACHE, SYSTEM DROP SCHEMA CACHE, DROP SCHEMA CACHE", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_FORMAT_SCHEMA_CACHE, "SYSTEM CLEAR FORMAT SCHEMA CACHE, SYSTEM DROP FORMAT SCHEMA CACHE, DROP FORMAT SCHEMA CACHE", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_S3_CLIENT_CACHE, "SYSTEM CLEAR S3 CLIENT CACHE, SYSTEM DROP S3 CLIENT, DROP S3 CLIENT CACHE", GLOBAL, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_TIME_SERIES_CACHES, "SYSTEM CLEAR TIME SERIES CACHES, SYSTEM DROP TIME SERIES CACHES, DROP TIME SERIES CACHES", TABLE, SYSTEM_DROP_CACHE, false) \
    M(SYSTEM_DROP_CACHE, "DROP CACHE", GROUP, SYSTEM, false) \
    M(SYSTEM_RELOAD_CONFIG, "RELOAD CONFIG", GLOBAL, SYSTEM_RELOAD, false) \
    M(SYSTEM_RELOAD_USERS, "RELOAD USERS", GLOBAL, SYSTEM_RELOAD, false) \
    M(SYSTEM_RELOAD_DICTIONARY, "SYSTEM RELOAD DICTIONARIES, RELOAD DICTIONARY, RELOAD DICTIONARIES, SYSTEM UNLOAD DICTIONARY, SYSTEM UNLOAD DICTIONARIES, UNLOAD DICTIONARY, UNLOAD DICTIONARIES", GLOBAL, SYSTEM_RELOAD, false) \
    M(SYSTEM_RELOAD_FUNCTION, "SYSTEM RELOAD FUNCTIONS, RELOAD FUNCTION, RELOAD FUNCTIONS", GLOBAL, SYSTEM_RELOAD, false) \
    M(SYSTEM_RELOAD_EMBEDDED_DICTIONARIES, "RELOAD EMBEDDED DICTIONARIES", GLOBAL, SYSTEM_RELOAD, false) /* implicitly enabled by the grant SYSTEM_RELOAD_DICTIONARY ON *.* */\
    M(SYSTEM_RELOAD_ASYNCHRONOUS_METRICS, "RELOAD ASYNCHRONOUS METRICS", GLOBAL, SYSTEM_RELOAD, false) \
    M(SYSTEM_RECONNECT_ZOOKEEPER, "SYSTEM RECONNECT ZOOKEEPER, RECONNECT ZOOKEEPER", GLOBAL, SYSTEM, false) \
    M(SYSTEM_RELOAD, "", GROUP, SYSTEM, false) \
    M(SYSTEM_RESTART_DISK, "SYSTEM RESTART DISK", GLOBAL, SYSTEM, false) \
    M(SYSTEM_WAIT_BLOBS_CLEANUP, "SYSTEM WAIT BLOBS CLEANUP", GLOBAL, SYSTEM, false) \
    M(SYSTEM_MERGES, "SYSTEM STOP MERGES, SYSTEM START MERGES, STOP MERGES, START MERGES", TABLE, SYSTEM, false) \
    M(SYSTEM_TTL_MERGES, "SYSTEM STOP TTL MERGES, SYSTEM START TTL MERGES, STOP TTL MERGES, START TTL MERGES", TABLE, SYSTEM, false) \
    M(SYSTEM_FETCHES, "SYSTEM STOP FETCHES, SYSTEM START FETCHES, STOP FETCHES, START FETCHES", TABLE, SYSTEM, false) \
    M(SYSTEM_MOVES, "SYSTEM STOP MOVES, SYSTEM START MOVES, STOP MOVES, START MOVES", TABLE, SYSTEM, false) \
    M(SYSTEM_PULLING_REPLICATION_LOG, "SYSTEM STOP PULLING REPLICATION LOG, SYSTEM START PULLING REPLICATION LOG", TABLE, SYSTEM, false) \
    M(SYSTEM_CLEANUP, "SYSTEM STOP CLEANUP, SYSTEM START CLEANUP", TABLE, SYSTEM, false) \
    M(SYSTEM_VIEWS, "SYSTEM REFRESH VIEW, SYSTEM START VIEWS, SYSTEM STOP VIEWS, SYSTEM START VIEW, SYSTEM STOP VIEW, SYSTEM PAUSE VIEWS, SYSTEM PAUSE VIEW, SYSTEM CANCEL VIEW, REFRESH VIEW, START VIEWS, STOP VIEWS, START VIEW, STOP VIEW, PAUSE VIEWS, PAUSE VIEW, CANCEL VIEW", VIEW, SYSTEM_BACKGROUND, false) \
    M(SYSTEM_STREAMING_ENGINES, "", TABLE, SYSTEM_BACKGROUND, false) /* (Kafka, RabbitMQ, NATS, S3Queue/AzureQueue) */ \
    M(SYSTEM_BACKGROUND, "", GROUP, SYSTEM, false) \
    M(SYSTEM_DISTRIBUTED_SENDS, "SYSTEM STOP DISTRIBUTED SENDS, SYSTEM START DISTRIBUTED SENDS, STOP DISTRIBUTED SENDS, START DISTRIBUTED SENDS", TABLE, SYSTEM_SENDS, false) \
    M(SYSTEM_REPLICATED_SENDS, "SYSTEM STOP REPLICATED SENDS, SYSTEM START REPLICATED SENDS, STOP REPLICATED SENDS, START REPLICATED SENDS", TABLE, SYSTEM_SENDS, false) \
    M(SYSTEM_SENDS, "SYSTEM STOP SENDS, SYSTEM START SENDS, STOP SENDS, START SENDS", GROUP, SYSTEM, false) \
    M(SYSTEM_REPLICATION_QUEUES, "SYSTEM STOP REPLICATION QUEUES, SYSTEM START REPLICATION QUEUES, STOP REPLICATION QUEUES, START REPLICATION QUEUES", TABLE, SYSTEM, false) \
    M(SYSTEM_VIRTUAL_PARTS_UPDATE, "SYSTEM STOP VIRTUAL PARTS UPDATE, SYSTEM START VIRTUAL PARTS UPDATE, STOP VIRTUAL PARTS UPDATE, START VIRTUAL PARTS UPDATE", TABLE, SYSTEM, false) \
    M(SYSTEM_REDUCE_BLOCKING_PARTS, "SYSTEM STOP REDUCE BLOCKING PARTS, SYSTEM START REDUCE BLOCKING PARTS, STOP REDUCE BLOCKING PARTS, START REDUCE BLOCKING PARTS", TABLE, SYSTEM, false) \
    M(SYSTEM_DROP_REPLICA, "DROP REPLICA", TABLE, SYSTEM, false) \
    M(SYSTEM_SYNC_REPLICA, "SYNC REPLICA", TABLE, SYSTEM, false) \
    M(SYSTEM_REPLICA_READINESS, "SYSTEM REPLICA READY, SYSTEM REPLICA UNREADY", GLOBAL, SYSTEM, false) \
    M(SYSTEM_RESTART_REPLICA, "RESTART REPLICA", TABLE, SYSTEM, false) \
    M(SYSTEM_RESTORE_REPLICA, "RESTORE REPLICA", TABLE, SYSTEM, false) \
    M(SYSTEM_RESTORE_DATABASE_REPLICA, "RESTORE DATABASE REPLICA", TABLE, SYSTEM, false) \
    M(SYSTEM_WAIT_LOADING_PARTS, "WAIT LOADING PARTS", TABLE, SYSTEM, false) \
    M(SYSTEM_WAIT_QUERY_RUNNER, "WAIT QUERY RUNNER", TABLE, SYSTEM, false) \
    M(SYSTEM_SYNC_DATABASE_REPLICA, "SYNC DATABASE REPLICA", DATABASE, SYSTEM, false) \
    M(SYSTEM_SYNC_TRANSACTION_LOG, "SYNC TRANSACTION LOG", GLOBAL, SYSTEM, false) \
    M(SYSTEM_SYNC_FILE_CACHE, "SYNC FILE CACHE", GLOBAL, SYSTEM, false) \
    M(SYSTEM_FLUSH_DISTRIBUTED, "FLUSH DISTRIBUTED", TABLE, SYSTEM_FLUSH, false) \
    M(SYSTEM_FLUSH_LOGS, "FLUSH LOGS", GLOBAL, SYSTEM_FLUSH, false) \
    M(SYSTEM_FLUSH_ASYNC_INSERT_QUEUE, "FLUSH ASYNC INSERT QUEUE", GLOBAL, SYSTEM_FLUSH, false) \
    M(SYSTEM_FLUSH_OBJECT_STORAGE_QUEUE, "FLUSH OBJECT STORAGE QUEUE", TABLE, SYSTEM_FLUSH, false) \
    M(SYSTEM_FLUSH, "", GROUP, SYSTEM, false) \
    M(SYSTEM_THREAD_FUZZER, "SYSTEM START THREAD FUZZER, SYSTEM STOP THREAD FUZZER, START THREAD FUZZER, STOP THREAD FUZZER", GLOBAL, SYSTEM, false) \
    M(SYSTEM_UNFREEZE, "SYSTEM UNFREEZE", GLOBAL, SYSTEM, false) \
    M(SYSTEM_UNLOCK_SNAPSHOT, "SYSTEM UNLOCK_SNAPSHOT", GLOBAL, SYSTEM, false) \
    M(SYSTEM_FAILPOINT, "SYSTEM ENABLE FAILPOINT, SYSTEM DISABLE FAILPOINT, SYSTEM WAIT FAILPOINT", GLOBAL, SYSTEM, false) \
    M(SYSTEM_MEMORY, "SYSTEM ALLOCATE MEMORY, SYSTEM FREE MEMORY", GLOBAL, SYSTEM, false) \
    M(SYSTEM_LISTEN, "SYSTEM START LISTEN, SYSTEM STOP LISTEN", GLOBAL, SYSTEM, false) \
    M(SYSTEM_JEMALLOC, "SYSTEM JEMALLOC PURGE, SYSTEM JEMALLOC ENABLE PROFILE, SYSTEM JEMALLOC DISABLE PROFILE, SYSTEM JEMALLOC FLUSH PROFILE", GLOBAL, SYSTEM, false) \
    M(SYSTEM_LOAD_PRIMARY_KEY, "SYSTEM LOAD PRIMARY KEY", TABLE, SYSTEM, false) \
    M(SYSTEM_UNLOAD_PRIMARY_KEY, "SYSTEM UNLOAD PRIMARY KEY", TABLE, SYSTEM, false) \
    M(SYSTEM_INSTRUMENT_ADD, "SYSTEM INSTRUMENT ADD", GLOBAL, SYSTEM, false) \
    M(SYSTEM_INSTRUMENT_REMOVE, "SYSTEM INSTRUMENT REMOVE", GLOBAL, SYSTEM, false) \
    M(SYSTEM_RESET_DDL_WORKER, "SYSTEM RESET DDL WORKER, RESET DDL WORKER", GLOBAL, SYSTEM, false) \
    M(SYSTEM, "", GROUP, ALL, false) /* allows to execute SYSTEM {SHUTDOWN|RELOAD CONFIG|...} */ \
    \
    M(dictGet, "dictHas, dictGetHierarchy, dictGetRoot, dictGetChildren, dictGetDescendants, dictIsIn", DICTIONARY, ALL, false) /* allows to execute functions dictGet(), dictHas(), dictGetHierarchy(), dictGetRoot(), dictGetChildren(), dictGetDescendants(), dictIsIn() */\
    M(displaySecretsInShowAndSelect, "", GLOBAL, ALL, false) /* allows to show plaintext secrets in SELECT and SHOW queries. display_secrets_in_show_and_select format and server settings must be turned on */\
    \
    M(addressToLine, "", GLOBAL, INTROSPECTION, false) /* allows to execute function addressToLine() */\
    M(addressToLineWithInlines, "", GLOBAL, INTROSPECTION, false) /* allows to execute function addressToLineWithInlines() */\
    M(addressToSymbol, "", GLOBAL, INTROSPECTION, false) /* allows to execute function addressToSymbol() */\
    M(demangle, "", GLOBAL, INTROSPECTION, false) /* allows to execute function demangle() */\
    M(INTROSPECTION, "INTROSPECTION FUNCTIONS", GROUP, ALL, false) /* allows to execute functions addressToLine(), addressToSymbol(), demangle()*/\
    \
    M(READ, "SOURCE READ", SOURCE, ALL, false) \
    M(WRITE, "SOURCE WRITE", SOURCE, ALL, false) \
    \
    M(CLUSTER, "", GLOBAL, ALL, false) /* ON CLUSTER queries */ \
    \
    /* Deprecated */ \
    M(FILE, "", GLOBAL, ALL, false) \
    M(URL, "", GLOBAL, ALL, false) \
    M(REMOTE, "", GLOBAL, ALL, false) \
    M(MONGO, "", GLOBAL, ALL, false) \
    M(REDIS, "", GLOBAL, ALL, false) \
    M(MYSQL, "", GLOBAL, ALL, false) \
    M(POSTGRES, "", GLOBAL, ALL, false) \
    M(SQLITE, "", GLOBAL, ALL, false) \
    M(ODBC, "", GLOBAL, ALL, false) \
    M(JDBC, "", GLOBAL, ALL, false) \
    M(HDFS, "", GLOBAL, ALL, false) \
    M(S3, "", GLOBAL, ALL, false) \
    M(HIVE, "", GLOBAL, ALL, false) \
    M(AZURE, "", GLOBAL, ALL, false) \
    M(KAFKA, "", GLOBAL, ALL, false) \
    M(NATS, "", GLOBAL, ALL, false) \
    M(RABBITMQ, "", GLOBAL, ALL, false) \
    M(YTSAURUS, "", GLOBAL, ALL, false) \
    M(ARROW_FLIGHT, "", GLOBAL, ALL, false) \
    M(BIGQUERY, "", GLOBAL, ALL, false) \
    M(DISK, "", GLOBAL, ALL, false) \
    M(SOURCES, "", GLOBAL, ALL, false) \
    \
    /* Obsolete */ \
    M_OBSOLETE(M, SYSTEM_RELOAD_MODEL, "SYSTEM RELOAD MODELS, RELOAD MODEL, RELOAD MODELS", GLOBAL, SYSTEM_RELOAD) /* the CatBoost integration was removed */ \
    /* Consts */ \
    M(ALL, "ALL PRIVILEGES", GROUP, NONE, false) /* full access */ \
    M(NONE, "USAGE, NO PRIVILEGES", GROUP, NONE, false) /* no access */

#define DECLARE_ACCESS_TYPE_ENUM_CONST(name, aliases, node_type, parent_group_name, is_obsolete) \
    name,

    APPLY_FOR_ACCESS_TYPES(DECLARE_ACCESS_TYPE_ENUM_CONST)
#undef DECLARE_ACCESS_TYPE_ENUM_CONST
};


std::string_view toString(AccessType type);

bool isObsolete(AccessType type);

}
