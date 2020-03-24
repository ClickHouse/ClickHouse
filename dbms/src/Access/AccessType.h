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
    NONE,  /// no access
    ALL,   /// full access

    SHOW_DATABASES,    /// allows to execute SHOW DATABASES, SHOW CREATE DATABASE, USE <database>
    SHOW_TABLES,       /// allows to execute SHOW TABLES, EXISTS <table>, CHECK <table>
    SHOW_COLUMNS,      /// allows to execute SHOW CREATE TABLE, DESCRIBE
    SHOW_DICTIONARIES, /// allows to execute SHOW DICTIONARIES, SHOW CREATE DICTIONARY, EXISTS <dictionary>
    SHOW,              /// allows to execute SHOW, USE, EXISTS, CHECK, DESCRIBE

    SELECT,
    INSERT,
    UPDATE,  /// allows to execute ALTER UPDATE
    DELETE,  /// allows to execute ALTER DELETE

    ADD_COLUMN,
    DROP_COLUMN,
    MODIFY_COLUMN,
    RENAME_COLUMN,
    COMMENT_COLUMN,
    CLEAR_COLUMN,
    ALTER_COLUMN,       /// allow to execute ALTER {ADD|DROP|MODIFY...} COLUMN

    ALTER_ORDER_BY,
    ADD_INDEX,
    DROP_INDEX,
    MATERIALIZE_INDEX,
    CLEAR_INDEX,
    INDEX,              /// allows to execute ALTER ORDER BY or ALTER {ADD|DROP...} INDEX

    ADD_CONSTRAINT,
    DROP_CONSTRAINT,
    ALTER_CONSTRAINT,   /// allows to execute ALTER {ADD|DROP} CONSTRAINT

    MODIFY_TTL,          /// allows to execute ALTER MODIFY TTL
    MATERIALIZE_TTL,     /// allows to execute ALTER MATERIALIZE TTL
    MODIFY_SETTING,      /// allows to execute ALTER MODIFY SETTING

    MOVE_PARTITION,
    FETCH_PARTITION,
    FREEZE_PARTITION,

    ALTER_TABLE,        /// allows to execute ALTER TABLE ...

    REFRESH_VIEW,       /// allows to execute ALTER LIVE VIEW REFRESH
    MODIFY_VIEW_QUERY,  /// allows to execute ALTER TABLE MODIFY QUERY
    ALTER_VIEW,         /// allows to execute ALTER LIVE VIEW REFRESH, ALTER TABLE MODIFY QUERY

    ALTER,              /// allows to execute ALTER {TABLE|LIVE VIEW} ...

    CREATE_DATABASE,        /// allows to execute {CREATE|ATTACH} DATABASE
    CREATE_TABLE,           /// allows to execute {CREATE|ATTACH} TABLE
    CREATE_VIEW,            /// allows to execute {CREATE|ATTACH} VIEW
    CREATE_DICTIONARY,      /// allows to execute {CREATE|ATTACH} DICTIONARY
    CREATE_TEMPORARY_TABLE, /// allows to create and manipulate temporary tables and views.
    CREATE,                 /// allows to execute {CREATE|ATTACH} [TEMPORARY] {DATABASE|TABLE|VIEW|DICTIONARY}

    DROP_DATABASE,
    DROP_TABLE,
    DROP_VIEW,
    DROP_DICTIONARY,
    DROP,               /// allows to execute DROP {DATABASE|TABLE|VIEW|DICTIONARY}

    TRUNCATE_TABLE,
    TRUNCATE_VIEW,
    TRUNCATE,           /// allows to execute TRUNCATE {TABLE|VIEW}

    OPTIMIZE,           /// allows to execute OPTIMIZE TABLE

    KILL_QUERY,         /// allows to kill a query started by another user (anyone can kill his own queries)

    CREATE_USER,
    ALTER_USER,
    DROP_USER,
    CREATE_ROLE,
    ALTER_ROLE,
    DROP_ROLE,
    CREATE_POLICY,
    ALTER_POLICY,
    DROP_POLICY,
    CREATE_QUOTA,
    ALTER_QUOTA,
    DROP_QUOTA,
    CREATE_SETTINGS_PROFILE,
    ALTER_SETTINGS_PROFILE,
    DROP_SETTINGS_PROFILE,

    ROLE_ADMIN,         /// allows to grant and revoke any roles.

    SHUTDOWN,
    DROP_CACHE,
    RELOAD_CONFIG,
    RELOAD_DICTIONARY,
    STOP_MERGES,
    STOP_TTL_MERGES,
    STOP_FETCHES,
    STOP_MOVES,
    STOP_DISTRIBUTED_SENDS,
    STOP_REPLICATED_SENDS,
    STOP_REPLICATION_QUEUES,
    SYNC_REPLICA,
    RESTART_REPLICA,
    FLUSH_DISTRIBUTED,
    FLUSH_LOGS,
    SYSTEM,                  /// allows to execute SYSTEM {SHUTDOWN|RELOAD CONFIG|...}

    dictGet,                 /// allows to execute functions dictGet, dictHas, dictGetHierarchy, dictIsIn
    dictHas,                 /// allows to execute functions dictGet, dictHas, dictGetHierarchy, dictIsIn
    dictGetHierarchy,        /// allows to execute functions dictGet, dictHas, dictGetHierarchy, dictIsIn
    dictIsIn,                /// allows to execute functions dictGet, dictHas, dictGetHierarchy, dictIsIn

    addressToLine,           /// allows to execute function addressToLine
    addressToSymbol,         /// allows to execute function addressToSymbol
    demangle,                /// allows to execute function demangle
    INTROSPECTION,           /// allows to execute functions addressToLine, addressToSymbol, demangle

    file,
    url,
    input,
    values,
    numbers,
    zeros,
    merge,
    remote,
    mysql,
    odbc,
    jdbc,
    hdfs,
    s3,
    TABLE_FUNCTIONS,  /// allows to execute any table function
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
        void addToMapping(AccessType type, const std::string_view & str)
        {
            String str2{str};
            boost::replace_all(str2, "_", " ");
            if (islower(str2[0]))
                str2 += "()";
            access_type_to_keyword_mapping[static_cast<size_t>(type)] = str2;
        }

        AccessTypeToKeywordConverter()
        {
#define ACCESS_TYPE_TO_KEYWORD_CASE(type) \
            addToMapping(AccessType::type, #type)

            ACCESS_TYPE_TO_KEYWORD_CASE(NONE);
            ACCESS_TYPE_TO_KEYWORD_CASE(ALL);

            ACCESS_TYPE_TO_KEYWORD_CASE(SHOW_DATABASES);
            ACCESS_TYPE_TO_KEYWORD_CASE(SHOW_TABLES);
            ACCESS_TYPE_TO_KEYWORD_CASE(SHOW_COLUMNS);
            ACCESS_TYPE_TO_KEYWORD_CASE(SHOW_DICTIONARIES);
            ACCESS_TYPE_TO_KEYWORD_CASE(SHOW);

            ACCESS_TYPE_TO_KEYWORD_CASE(SELECT);
            ACCESS_TYPE_TO_KEYWORD_CASE(INSERT);
            ACCESS_TYPE_TO_KEYWORD_CASE(UPDATE);
            ACCESS_TYPE_TO_KEYWORD_CASE(DELETE);

            ACCESS_TYPE_TO_KEYWORD_CASE(ADD_COLUMN);
            ACCESS_TYPE_TO_KEYWORD_CASE(DROP_COLUMN);
            ACCESS_TYPE_TO_KEYWORD_CASE(MODIFY_COLUMN);
            ACCESS_TYPE_TO_KEYWORD_CASE(RENAME_COLUMN);
            ACCESS_TYPE_TO_KEYWORD_CASE(COMMENT_COLUMN);
            ACCESS_TYPE_TO_KEYWORD_CASE(CLEAR_COLUMN);
            ACCESS_TYPE_TO_KEYWORD_CASE(ALTER_COLUMN);

            ACCESS_TYPE_TO_KEYWORD_CASE(ALTER_ORDER_BY);
            ACCESS_TYPE_TO_KEYWORD_CASE(ADD_INDEX);
            ACCESS_TYPE_TO_KEYWORD_CASE(DROP_INDEX);
            ACCESS_TYPE_TO_KEYWORD_CASE(MATERIALIZE_INDEX);
            ACCESS_TYPE_TO_KEYWORD_CASE(CLEAR_INDEX);
            ACCESS_TYPE_TO_KEYWORD_CASE(INDEX);

            ACCESS_TYPE_TO_KEYWORD_CASE(ADD_CONSTRAINT);
            ACCESS_TYPE_TO_KEYWORD_CASE(DROP_CONSTRAINT);
            ACCESS_TYPE_TO_KEYWORD_CASE(ALTER_CONSTRAINT);

            ACCESS_TYPE_TO_KEYWORD_CASE(MODIFY_TTL);
            ACCESS_TYPE_TO_KEYWORD_CASE(MATERIALIZE_TTL);
            ACCESS_TYPE_TO_KEYWORD_CASE(MODIFY_SETTING);

            ACCESS_TYPE_TO_KEYWORD_CASE(MOVE_PARTITION);
            ACCESS_TYPE_TO_KEYWORD_CASE(FETCH_PARTITION);
            ACCESS_TYPE_TO_KEYWORD_CASE(FREEZE_PARTITION);

            ACCESS_TYPE_TO_KEYWORD_CASE(ALTER_TABLE);

            ACCESS_TYPE_TO_KEYWORD_CASE(REFRESH_VIEW);
            ACCESS_TYPE_TO_KEYWORD_CASE(MODIFY_VIEW_QUERY);
            ACCESS_TYPE_TO_KEYWORD_CASE(ALTER_VIEW);

            ACCESS_TYPE_TO_KEYWORD_CASE(ALTER);

            ACCESS_TYPE_TO_KEYWORD_CASE(CREATE_DATABASE);
            ACCESS_TYPE_TO_KEYWORD_CASE(CREATE_TABLE);
            ACCESS_TYPE_TO_KEYWORD_CASE(CREATE_VIEW);
            ACCESS_TYPE_TO_KEYWORD_CASE(CREATE_DICTIONARY);
            ACCESS_TYPE_TO_KEYWORD_CASE(CREATE_TEMPORARY_TABLE);
            ACCESS_TYPE_TO_KEYWORD_CASE(CREATE);

            ACCESS_TYPE_TO_KEYWORD_CASE(DROP_DATABASE);
            ACCESS_TYPE_TO_KEYWORD_CASE(DROP_TABLE);
            ACCESS_TYPE_TO_KEYWORD_CASE(DROP_VIEW);
            ACCESS_TYPE_TO_KEYWORD_CASE(DROP_DICTIONARY);
            ACCESS_TYPE_TO_KEYWORD_CASE(DROP);

            ACCESS_TYPE_TO_KEYWORD_CASE(TRUNCATE_TABLE);
            ACCESS_TYPE_TO_KEYWORD_CASE(TRUNCATE_VIEW);
            ACCESS_TYPE_TO_KEYWORD_CASE(TRUNCATE);

            ACCESS_TYPE_TO_KEYWORD_CASE(OPTIMIZE);

            ACCESS_TYPE_TO_KEYWORD_CASE(KILL_QUERY);

            ACCESS_TYPE_TO_KEYWORD_CASE(CREATE_USER);
            ACCESS_TYPE_TO_KEYWORD_CASE(ALTER_USER);
            ACCESS_TYPE_TO_KEYWORD_CASE(DROP_USER);
            ACCESS_TYPE_TO_KEYWORD_CASE(CREATE_ROLE);
            ACCESS_TYPE_TO_KEYWORD_CASE(ALTER_ROLE);
            ACCESS_TYPE_TO_KEYWORD_CASE(DROP_ROLE);
            ACCESS_TYPE_TO_KEYWORD_CASE(CREATE_POLICY);
            ACCESS_TYPE_TO_KEYWORD_CASE(ALTER_POLICY);
            ACCESS_TYPE_TO_KEYWORD_CASE(DROP_POLICY);
            ACCESS_TYPE_TO_KEYWORD_CASE(CREATE_QUOTA);
            ACCESS_TYPE_TO_KEYWORD_CASE(ALTER_QUOTA);
            ACCESS_TYPE_TO_KEYWORD_CASE(DROP_QUOTA);
            ACCESS_TYPE_TO_KEYWORD_CASE(CREATE_SETTINGS_PROFILE);
            ACCESS_TYPE_TO_KEYWORD_CASE(ALTER_SETTINGS_PROFILE);
            ACCESS_TYPE_TO_KEYWORD_CASE(DROP_SETTINGS_PROFILE);
            ACCESS_TYPE_TO_KEYWORD_CASE(ROLE_ADMIN);

            ACCESS_TYPE_TO_KEYWORD_CASE(SHUTDOWN);
            ACCESS_TYPE_TO_KEYWORD_CASE(DROP_CACHE);
            ACCESS_TYPE_TO_KEYWORD_CASE(RELOAD_CONFIG);
            ACCESS_TYPE_TO_KEYWORD_CASE(RELOAD_DICTIONARY);
            ACCESS_TYPE_TO_KEYWORD_CASE(STOP_MERGES);
            ACCESS_TYPE_TO_KEYWORD_CASE(STOP_TTL_MERGES);
            ACCESS_TYPE_TO_KEYWORD_CASE(STOP_FETCHES);
            ACCESS_TYPE_TO_KEYWORD_CASE(STOP_MOVES);
            ACCESS_TYPE_TO_KEYWORD_CASE(STOP_DISTRIBUTED_SENDS);
            ACCESS_TYPE_TO_KEYWORD_CASE(STOP_REPLICATED_SENDS);
            ACCESS_TYPE_TO_KEYWORD_CASE(STOP_REPLICATION_QUEUES);
            ACCESS_TYPE_TO_KEYWORD_CASE(SYNC_REPLICA);
            ACCESS_TYPE_TO_KEYWORD_CASE(RESTART_REPLICA);
            ACCESS_TYPE_TO_KEYWORD_CASE(FLUSH_DISTRIBUTED);
            ACCESS_TYPE_TO_KEYWORD_CASE(FLUSH_LOGS);
            ACCESS_TYPE_TO_KEYWORD_CASE(SYSTEM);

            ACCESS_TYPE_TO_KEYWORD_CASE(dictGet);
            ACCESS_TYPE_TO_KEYWORD_CASE(dictHas);
            ACCESS_TYPE_TO_KEYWORD_CASE(dictGetHierarchy);
            ACCESS_TYPE_TO_KEYWORD_CASE(dictIsIn);

            ACCESS_TYPE_TO_KEYWORD_CASE(addressToLine);
            ACCESS_TYPE_TO_KEYWORD_CASE(addressToSymbol);
            ACCESS_TYPE_TO_KEYWORD_CASE(demangle);
            ACCESS_TYPE_TO_KEYWORD_CASE(INTROSPECTION);

            ACCESS_TYPE_TO_KEYWORD_CASE(file);
            ACCESS_TYPE_TO_KEYWORD_CASE(url);
            ACCESS_TYPE_TO_KEYWORD_CASE(input);
            ACCESS_TYPE_TO_KEYWORD_CASE(values);
            ACCESS_TYPE_TO_KEYWORD_CASE(numbers);
            ACCESS_TYPE_TO_KEYWORD_CASE(zeros);
            ACCESS_TYPE_TO_KEYWORD_CASE(merge);
            ACCESS_TYPE_TO_KEYWORD_CASE(remote);
            ACCESS_TYPE_TO_KEYWORD_CASE(mysql);
            ACCESS_TYPE_TO_KEYWORD_CASE(odbc);
            ACCESS_TYPE_TO_KEYWORD_CASE(jdbc);
            ACCESS_TYPE_TO_KEYWORD_CASE(hdfs);
            ACCESS_TYPE_TO_KEYWORD_CASE(s3);
            ACCESS_TYPE_TO_KEYWORD_CASE(TABLE_FUNCTIONS);

#undef ACCESS_TYPE_TO_KEYWORD_CASE
        }

        std::array<String, MAX_ACCESS_TYPE> access_type_to_keyword_mapping;
    };
}

inline std::string_view toKeyword(AccessType type) { return impl::AccessTypeToKeywordConverter<>::instance().convert(type); }

}
