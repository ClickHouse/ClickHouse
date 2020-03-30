#pragma once

#include <Access/AccessType.h>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <ext/range.h>
#include <ext/push_back.h>
#include <bitset>
#include <unordered_map>


namespace DB
{
/// Represents a combination of access types which can be granted globally, on databases, tables, columns, etc.
/// For example "SELECT, CREATE USER" is an access type.
class AccessFlags
{
public:
    AccessFlags(AccessType type);

    /// The same as AccessFlags(AccessType::NONE).
    AccessFlags() = default;

    /// Constructs from a string like "SELECT".
    AccessFlags(const std::string_view & keyword);

    /// Constructs from a list of strings like "SELECT, UPDATE, INSERT".
    AccessFlags(const std::vector<std::string_view> & keywords);
    AccessFlags(const Strings & keywords);

    AccessFlags(const AccessFlags & src) = default;
    AccessFlags(AccessFlags && src) = default;
    AccessFlags & operator =(const AccessFlags & src) = default;
    AccessFlags & operator =(AccessFlags && src) = default;

    /// Returns the access type which contains two specified access types.
    AccessFlags & operator |=(const AccessFlags & other) { flags |= other.flags; return *this; }
    friend AccessFlags operator |(const AccessFlags & left, const AccessFlags & right) { return AccessFlags(left) |= right; }

    /// Returns the access type which contains the common part of two access types.
    AccessFlags & operator &=(const AccessFlags & other) { flags &= other.flags; return *this; }
    friend AccessFlags operator &(const AccessFlags & left, const AccessFlags & right) { return AccessFlags(left) &= right; }

    /// Returns the access type which contains only the part of the first access type which is not the part of the second access type.
    /// (lhs - rhs) is the same as (lhs & ~rhs).
    AccessFlags & operator -=(const AccessFlags & other) { flags &= ~other.flags; return *this; }
    friend AccessFlags operator -(const AccessFlags & left, const AccessFlags & right) { return AccessFlags(left) -= right; }

    AccessFlags operator ~() const { AccessFlags res; res.flags = ~flags; return res; }

    bool isEmpty() const { return flags.none(); }
    explicit operator bool() const { return !isEmpty(); }
    bool contains(const AccessFlags & other) const { return (flags & other.flags) == other.flags; }

    friend bool operator ==(const AccessFlags & left, const AccessFlags & right) { return left.flags == right.flags; }
    friend bool operator !=(const AccessFlags & left, const AccessFlags & right) { return !(left == right); }

    void clear() { flags.reset(); }

    /// Returns a comma-separated list of keywords, like "SELECT, CREATE USER, UPDATE".
    String toString() const;

    /// Returns a list of keywords.
    std::vector<std::string_view> toKeywords() const;

    /// Returns all the flags.
    /// These are the same as (allGlobalFlags() | allDatabaseFlags() | allTableFlags() | allColumnsFlags() | allDictionaryFlags()).
    static AccessFlags allFlags();

    /// Returns all the global flags.
    static AccessFlags allGlobalFlags();

    /// Returns all the flags related to a database.
    static AccessFlags allDatabaseFlags();

    /// Returns all the flags related to a table.
    static AccessFlags allTableFlags();

    /// Returns all the flags related to a column.
    static AccessFlags allColumnFlags();

    /// Returns all the flags related to a dictionary.
    static AccessFlags allDictionaryFlags();

private:
    static constexpr size_t NUM_FLAGS = 128;
    using Flags = std::bitset<NUM_FLAGS>;
    Flags flags;

    AccessFlags(const Flags & flags_) : flags(flags_) {}

    template <typename = void>
    class Impl;
};


namespace ErrorCodes
{
    extern const int UNKNOWN_ACCESS_TYPE;
}

template <typename>
class AccessFlags::Impl
{
public:
    static const Impl & instance()
    {
        static const Impl res;
        return res;
    }

    Flags accessTypeToFlags(AccessType type) const
    {
        return access_type_to_flags_mapping[static_cast<size_t>(type)];
    }

    Flags keywordToFlags(const std::string_view & keyword) const
    {
        auto it = keyword_to_flags_map.find(keyword);
        if (it == keyword_to_flags_map.end())
        {
            String uppercased_keyword{keyword};
            boost::to_upper(uppercased_keyword);
            it = keyword_to_flags_map.find(uppercased_keyword);
            if (it == keyword_to_flags_map.end())
                throw Exception("Unknown access type: " + String(keyword), ErrorCodes::UNKNOWN_ACCESS_TYPE);
        }
        return it->second;
    }

    Flags keywordsToFlags(const std::vector<std::string_view> & keywords) const
    {
        Flags res;
        for (const auto & keyword : keywords)
            res |= keywordToFlags(keyword);
        return res;
    }

    Flags keywordsToFlags(const Strings & keywords) const
    {
        Flags res;
        for (const auto & keyword : keywords)
            res |= keywordToFlags(keyword);
        return res;
    }

    std::vector<std::string_view> flagsToKeywords(const Flags & flags_) const
    {
        std::vector<std::string_view> keywords;
        flagsToKeywordsRec(flags_, keywords, *flags_to_keyword_tree);

        if (keywords.empty())
            keywords.push_back("USAGE");

        return keywords;
    }

    String flagsToString(const Flags & flags_) const
    {
        String str;
        for (const auto & keyword : flagsToKeywords(flags_))
        {
            if (!str.empty())
                str += ", ";
            str += keyword;
        }
        return str;
    }

    const Flags & getAllFlags() const { return all_flags; }
    const Flags & getGlobalFlags() const { return all_flags_for_target[GLOBAL]; }
    const Flags & getDatabaseFlags() const { return all_flags_for_target[DATABASE]; }
    const Flags & getTableFlags() const { return all_flags_for_target[TABLE]; }
    const Flags & getColumnFlags() const { return all_flags_for_target[COLUMN]; }
    const Flags & getDictionaryFlags() const { return all_flags_for_target[DICTIONARY]; }

private:
    enum Target
    {
        UNKNOWN_TARGET,
        GLOBAL,
        DATABASE,
        TABLE,
        VIEW = TABLE,
        COLUMN,
        DICTIONARY,
    };

    static constexpr size_t NUM_TARGETS = static_cast<size_t>(DICTIONARY) + 1;

    struct Node;
    using NodePtr = std::unique_ptr<Node>;
    using Nodes = std::vector<NodePtr>;

    template <typename... Args>
    static Nodes nodes(Args&& ... args)
    {
        Nodes res;
        ext::push_back(res, std::move(args)...);
        return res;
    }

    struct Node
    {
        std::string_view keyword;
        std::vector<String> aliases;
        Flags flags;
        Target target = UNKNOWN_TARGET;
        Nodes children;

        Node(std::string_view keyword_, size_t flag_, Target target_)
            : keyword(keyword_), target(target_)
        {
            flags.set(flag_);
        }

        Node(std::string_view keyword_, Nodes children_)
            : keyword(keyword_), children(std::move(children_))
        {
            for (const auto & child : children)
                flags |= child->flags;
        }

        template <typename... Args>
        Node(std::string_view keyword_, NodePtr first_child, Args &&... other_children)
            : Node(keyword_, nodes(std::move(first_child), std::move(other_children)...)) {}
    };

    static void flagsToKeywordsRec(const Flags & flags_, std::vector<std::string_view> & keywords, const Node & start_node)
    {
        Flags matching_flags = (flags_ & start_node.flags);
        if (matching_flags.any())
        {
            if (matching_flags == start_node.flags)
            {
                keywords.push_back(start_node.keyword);
            }
            else
            {
                for (const auto & child : start_node.children)
                   flagsToKeywordsRec(flags_, keywords, *child);
            }
        }
    }

    static NodePtr makeFlagsToKeywordTree()
    {
        size_t next_flag = 0;
        Nodes all;

        auto show_databases = std::make_unique<Node>("SHOW DATABASES", next_flag++, DATABASE);
        auto show_tables = std::make_unique<Node>("SHOW TABLES", next_flag++, TABLE);
        auto show_columns = std::make_unique<Node>("SHOW COLUMNS", next_flag++, COLUMN);
        auto show_dictionaries = std::make_unique<Node>("SHOW DICTIONARIES", next_flag++, DICTIONARY);
        auto show = std::make_unique<Node>("SHOW", std::move(show_databases), std::move(show_tables), std::move(show_columns), std::move(show_dictionaries));
        ext::push_back(all, std::move(show));

        auto select = std::make_unique<Node>("SELECT", next_flag++, COLUMN);
        auto insert = std::make_unique<Node>("INSERT", next_flag++, COLUMN);
        ext::push_back(all, std::move(select), std::move(insert));

        auto update = std::make_unique<Node>("UPDATE", next_flag++, COLUMN);
        ext::push_back(update->aliases, "ALTER UPDATE");
        auto delet = std::make_unique<Node>("DELETE", next_flag++, TABLE);
        ext::push_back(delet->aliases, "ALTER DELETE");

        auto add_column = std::make_unique<Node>("ADD COLUMN", next_flag++, COLUMN);
        add_column->aliases.push_back("ALTER ADD COLUMN");
        auto modify_column = std::make_unique<Node>("MODIFY COLUMN", next_flag++, COLUMN);
        modify_column->aliases.push_back("ALTER MODIFY COLUMN");
        auto drop_column = std::make_unique<Node>("DROP COLUMN", next_flag++, COLUMN);
        drop_column->aliases.push_back("ALTER DROP COLUMN");
        auto comment_column = std::make_unique<Node>("COMMENT COLUMN", next_flag++, COLUMN);
        comment_column->aliases.push_back("ALTER COMMENT COLUMN");
        auto clear_column = std::make_unique<Node>("CLEAR COLUMN", next_flag++, COLUMN);
        clear_column->aliases.push_back("ALTER CLEAR COLUMN");
        auto alter_column = std::make_unique<Node>("ALTER COLUMN", std::move(add_column), std::move(modify_column), std::move(drop_column), std::move(comment_column), std::move(clear_column));

        auto alter_order_by = std::make_unique<Node>("ALTER ORDER BY", next_flag++, TABLE);
        alter_order_by->aliases.push_back("MODIFY ORDER BY");
        alter_order_by->aliases.push_back("ALTER MODIFY ORDER BY");
        auto add_index = std::make_unique<Node>("ADD INDEX", next_flag++, TABLE);
        add_index->aliases.push_back("ALTER ADD INDEX");
        auto drop_index = std::make_unique<Node>("DROP INDEX", next_flag++, TABLE);
        drop_index->aliases.push_back("ALTER DROP INDEX");
        auto materialize_index = std::make_unique<Node>("MATERIALIZE INDEX", next_flag++, TABLE);
        materialize_index->aliases.push_back("ALTER MATERIALIZE INDEX");
        auto clear_index = std::make_unique<Node>("CLEAR INDEX", next_flag++, TABLE);
        clear_index->aliases.push_back("ALTER CLEAR INDEX");
        auto index = std::make_unique<Node>("INDEX", std::move(alter_order_by), std::move(add_index), std::move(drop_index), std::move(materialize_index), std::move(clear_index));
        index->aliases.push_back("ALTER INDEX");

        auto add_constraint = std::make_unique<Node>("ADD CONSTRAINT", next_flag++, TABLE);
        add_constraint->aliases.push_back("ALTER ADD CONSTRAINT");
        auto drop_constraint = std::make_unique<Node>("DROP CONSTRAINT", next_flag++, TABLE);
        drop_constraint->aliases.push_back("ALTER DROP CONSTRAINT");
        auto alter_constraint = std::make_unique<Node>("CONSTRAINT", std::move(add_constraint), std::move(drop_constraint));
        alter_constraint->aliases.push_back("ALTER CONSTRAINT");

        auto modify_ttl = std::make_unique<Node>("MODIFY TTL", next_flag++, TABLE);
        modify_ttl->aliases.push_back("ALTER MODIFY TTL");
        auto materialize_ttl = std::make_unique<Node>("MATERIALIZE TTL", next_flag++, TABLE);
        materialize_ttl->aliases.push_back("ALTER MATERIALIZE TTL");

        auto modify_setting = std::make_unique<Node>("MODIFY SETTING", next_flag++, TABLE);
        modify_setting->aliases.push_back("ALTER MODIFY SETTING");

        auto move_partition = std::make_unique<Node>("MOVE PARTITION", next_flag++, TABLE);
        ext::push_back(move_partition->aliases, "ALTER MOVE PARTITION", "MOVE PART", "ALTER MOVE PART");
        auto fetch_partition = std::make_unique<Node>("FETCH PARTITION", next_flag++, TABLE);
        ext::push_back(fetch_partition->aliases, "ALTER FETCH PARTITION");
        auto freeze_partition = std::make_unique<Node>("FREEZE PARTITION", next_flag++, TABLE);
        ext::push_back(freeze_partition->aliases, "ALTER FREEZE PARTITION");

        auto alter_table = std::make_unique<Node>("ALTER TABLE", std::move(update), std::move(delet), std::move(alter_column), std::move(index), std::move(alter_constraint), std::move(modify_ttl), std::move(materialize_ttl), std::move(modify_setting), std::move(move_partition), std::move(fetch_partition), std::move(freeze_partition));

        auto refresh_view = std::make_unique<Node>("REFRESH VIEW", next_flag++, VIEW);
        ext::push_back(refresh_view->aliases, "ALTER LIVE VIEW REFRESH");
        auto modify_view_query = std::make_unique<Node>("MODIFY VIEW QUERY", next_flag++, VIEW);
        auto alter_view = std::make_unique<Node>("ALTER VIEW", std::move(refresh_view), std::move(modify_view_query));

        auto alter = std::make_unique<Node>("ALTER", std::move(alter_table), std::move(alter_view));
        ext::push_back(all, std::move(alter));

        auto create_database = std::make_unique<Node>("CREATE DATABASE", next_flag++, DATABASE);
        auto create_table = std::make_unique<Node>("CREATE TABLE", next_flag++, TABLE);
        auto create_view = std::make_unique<Node>("CREATE VIEW", next_flag++, VIEW);
        auto create_dictionary = std::make_unique<Node>("CREATE DICTIONARY", next_flag++, DICTIONARY);
        auto create = std::make_unique<Node>("CREATE", std::move(create_database), std::move(create_table), std::move(create_view), std::move(create_dictionary));
        ext::push_back(all, std::move(create));

        auto create_temporary_table = std::make_unique<Node>("CREATE TEMPORARY TABLE", next_flag++, GLOBAL);
        ext::push_back(all, std::move(create_temporary_table));

        auto drop_database = std::make_unique<Node>("DROP DATABASE", next_flag++, DATABASE);
        auto drop_table = std::make_unique<Node>("DROP TABLE", next_flag++, TABLE);
        auto drop_view = std::make_unique<Node>("DROP VIEW", next_flag++, VIEW);
        auto drop_dictionary = std::make_unique<Node>("DROP DICTIONARY", next_flag++, DICTIONARY);
        auto drop = std::make_unique<Node>("DROP", std::move(drop_database), std::move(drop_table), std::move(drop_view), std::move(drop_dictionary));
        ext::push_back(all, std::move(drop));

        auto truncate_table = std::make_unique<Node>("TRUNCATE TABLE", next_flag++, TABLE);
        auto truncate_view = std::make_unique<Node>("TRUNCATE VIEW", next_flag++, VIEW);
        auto truncate = std::make_unique<Node>("TRUNCATE", std::move(truncate_table), std::move(truncate_view));
        ext::push_back(all, std::move(truncate));

        auto optimize = std::make_unique<Node>("OPTIMIZE", next_flag++, TABLE);
        optimize->aliases.push_back("OPTIMIZE TABLE");
        ext::push_back(all, std::move(optimize));

        auto kill_query = std::make_unique<Node>("KILL QUERY", next_flag++, GLOBAL);
        ext::push_back(all, std::move(kill_query));

        auto create_user = std::make_unique<Node>("CREATE USER", next_flag++, GLOBAL);
        auto alter_user = std::make_unique<Node>("ALTER USER", next_flag++, GLOBAL);
        auto drop_user = std::make_unique<Node>("DROP USER", next_flag++, GLOBAL);
        auto create_role = std::make_unique<Node>("CREATE ROLE", next_flag++, GLOBAL);
        auto alter_role = std::make_unique<Node>("ALTER ROLE", next_flag++, GLOBAL);
        auto drop_role = std::make_unique<Node>("DROP ROLE", next_flag++, GLOBAL);
        auto create_policy = std::make_unique<Node>("CREATE POLICY", next_flag++, GLOBAL);
        auto alter_policy = std::make_unique<Node>("ALTER POLICY", next_flag++, GLOBAL);
        auto drop_policy = std::make_unique<Node>("DROP POLICY", next_flag++, GLOBAL);
        auto create_quota = std::make_unique<Node>("CREATE QUOTA", next_flag++, GLOBAL);
        auto alter_quota = std::make_unique<Node>("ALTER QUOTA", next_flag++, GLOBAL);
        auto drop_quota = std::make_unique<Node>("DROP QUOTA", next_flag++, GLOBAL);
        auto create_profile = std::make_unique<Node>("CREATE SETTINGS PROFILE", next_flag++, GLOBAL);
        ext::push_back(create_profile->aliases, "CREATE PROFILE");
        auto alter_profile = std::make_unique<Node>("ALTER SETTINGS PROFILE", next_flag++, GLOBAL);
        ext::push_back(alter_profile->aliases, "ALTER PROFILE");
        auto drop_profile = std::make_unique<Node>("DROP SETTINGS PROFILE", next_flag++, GLOBAL);
        ext::push_back(drop_profile->aliases, "DROP PROFILE");
        auto role_admin = std::make_unique<Node>("ROLE ADMIN", next_flag++, GLOBAL);
        ext::push_back(all, std::move(create_user), std::move(alter_user), std::move(drop_user), std::move(create_role), std::move(alter_role), std::move(drop_role), std::move(create_policy), std::move(alter_policy), std::move(drop_policy), std::move(create_quota), std::move(alter_quota), std::move(drop_quota), std::move(create_profile), std::move(alter_profile), std::move(drop_profile), std::move(role_admin));

        auto shutdown = std::make_unique<Node>("SHUTDOWN", next_flag++, GLOBAL);
        ext::push_back(shutdown->aliases, "SYSTEM SHUTDOWN", "SYSTEM KILL");
        auto drop_cache = std::make_unique<Node>("DROP CACHE", next_flag++, GLOBAL);
        ext::push_back(drop_cache->aliases, "SYSTEM DROP CACHE", "DROP DNS CACHE", "SYSTEM DROP DNS CACHE", "DROP MARK CACHE", "SYSTEM DROP MARK CACHE", "DROP UNCOMPRESSED CACHE", "SYSTEM DROP UNCOMPRESSED CACHE", "DROP COMPILED EXPRESSION CACHE", "SYSTEM DROP COMPILED EXPRESSION CACHE");
        auto reload_config = std::make_unique<Node>("RELOAD CONFIG", next_flag++, GLOBAL);
        ext::push_back(reload_config->aliases, "SYSTEM RELOAD CONFIG");
        auto reload_dictionary = std::make_unique<Node>("RELOAD DICTIONARY", next_flag++, GLOBAL);
        ext::push_back(reload_dictionary->aliases, "SYSTEM RELOAD DICTIONARY", "RELOAD DICTIONARIES", "SYSTEM RELOAD DICTIONARIES", "RELOAD EMBEDDED DICTIONARIES", "SYSTEM RELOAD EMBEDDED DICTIONARIES");
        auto stop_merges = std::make_unique<Node>("STOP MERGES", next_flag++, TABLE);
        ext::push_back(stop_merges->aliases, "SYSTEM STOP MERGES", "START MERGES", "SYSTEM START MERGES");
        auto stop_ttl_merges = std::make_unique<Node>("STOP TTL MERGES", next_flag++, TABLE);
        ext::push_back(stop_ttl_merges->aliases, "SYSTEM STOP TTL MERGES", "START TTL MERGES", "SYSTEM START TTL MERGES");
        auto stop_fetches = std::make_unique<Node>("STOP FETCHES", next_flag++, TABLE);
        ext::push_back(stop_fetches->aliases, "SYSTEM STOP FETCHES", "START FETCHES", "SYSTEM START FETCHES");
        auto stop_moves = std::make_unique<Node>("STOP MOVES", next_flag++, TABLE);
        ext::push_back(stop_moves->aliases, "SYSTEM STOP MOVES", "START MOVES", "SYSTEM START MOVES");
        auto stop_distributed_sends = std::make_unique<Node>("STOP DISTRIBUTED SENDS", next_flag++, TABLE);
        ext::push_back(stop_distributed_sends->aliases, "SYSTEM STOP DISTRIBUTED SENDS", "START DISTRIBUTED SENDS", "SYSTEM START DISTRIBUTED SENDS");
        auto stop_replicated_sends = std::make_unique<Node>("STOP REPLICATED SENDS", next_flag++, TABLE);
        ext::push_back(stop_replicated_sends->aliases, "SYSTEM STOP REPLICATED SENDS", "START REPLICATED SENDS", "SYSTEM START REPLICATED SENDS");
        auto stop_replication_queues = std::make_unique<Node>("STOP REPLICATION QUEUES", next_flag++, TABLE);
        ext::push_back(stop_replication_queues->aliases, "SYSTEM STOP REPLICATION QUEUES", "START REPLICATION QUEUES", "SYSTEM START REPLICATION QUEUES");
        auto sync_replica = std::make_unique<Node>("SYNC REPLICA", next_flag++, TABLE);
        ext::push_back(sync_replica->aliases, "SYSTEM SYNC REPLICA");
        auto restart_replica = std::make_unique<Node>("RESTART REPLICA", next_flag++, TABLE);
        ext::push_back(restart_replica->aliases, "SYSTEM RESTART REPLICA");
        auto flush_distributed = std::make_unique<Node>("FLUSH DISTRIBUTED", next_flag++, TABLE);
        ext::push_back(flush_distributed->aliases, "SYSTEM FLUSH DISTRIBUTED");
        auto flush_logs = std::make_unique<Node>("FLUSH LOGS", next_flag++, GLOBAL);
        ext::push_back(flush_logs->aliases, "SYSTEM FLUSH LOGS");
        auto system = std::make_unique<Node>("SYSTEM", std::move(shutdown), std::move(drop_cache), std::move(reload_config), std::move(reload_dictionary), std::move(stop_merges), std::move(stop_ttl_merges), std::move(stop_fetches), std::move(stop_moves), std::move(stop_distributed_sends), std::move(stop_replicated_sends), std::move(stop_replication_queues), std::move(sync_replica), std::move(restart_replica), std::move(flush_distributed), std::move(flush_logs));
        ext::push_back(all, std::move(system));

        auto dict_get = std::make_unique<Node>("dictGet()", next_flag++, DICTIONARY);
        dict_get->aliases.push_back("dictHas()");
        dict_get->aliases.push_back("dictGetHierarchy()");
        dict_get->aliases.push_back("dictIsIn()");
        ext::push_back(all, std::move(dict_get));

        auto address_to_line = std::make_unique<Node>("addressToLine()", next_flag++, GLOBAL);
        auto address_to_symbol = std::make_unique<Node>("addressToSymbol()", next_flag++, GLOBAL);
        auto demangle = std::make_unique<Node>("demangle()", next_flag++, GLOBAL);
        auto introspection = std::make_unique<Node>("INTROSPECTION", std::move(address_to_line), std::move(address_to_symbol), std::move(demangle));
        ext::push_back(introspection->aliases, "INTROSPECTION FUNCTIONS");
        ext::push_back(all, std::move(introspection));

        auto file = std::make_unique<Node>("file()", next_flag++, GLOBAL);
        auto url = std::make_unique<Node>("url()", next_flag++, GLOBAL);
        auto input = std::make_unique<Node>("input()", next_flag++, GLOBAL);
        auto values = std::make_unique<Node>("values()", next_flag++, GLOBAL);
        auto numbers = std::make_unique<Node>("numbers()", next_flag++, GLOBAL);
        auto zeros = std::make_unique<Node>("zeros()", next_flag++, GLOBAL);
        auto merge = std::make_unique<Node>("merge()", next_flag++, DATABASE);
        auto remote = std::make_unique<Node>("remote()", next_flag++, GLOBAL);
        ext::push_back(remote->aliases, "remoteSecure()", "cluster()");
        auto mysql = std::make_unique<Node>("mysql()", next_flag++, GLOBAL);
        auto odbc = std::make_unique<Node>("odbc()", next_flag++, GLOBAL);
        auto jdbc = std::make_unique<Node>("jdbc()", next_flag++, GLOBAL);
        auto hdfs = std::make_unique<Node>("hdfs()", next_flag++, GLOBAL);
        auto s3 = std::make_unique<Node>("s3()", next_flag++, GLOBAL);
        auto table_functions = std::make_unique<Node>("TABLE FUNCTIONS", std::move(file), std::move(url), std::move(input), std::move(values), std::move(numbers), std::move(zeros), std::move(merge), std::move(remote), std::move(mysql), std::move(odbc), std::move(jdbc), std::move(hdfs), std::move(s3));
        ext::push_back(all, std::move(table_functions));

        auto node_all = std::make_unique<Node>("ALL", std::move(all));
        node_all->aliases.push_back("ALL PRIVILEGES");
        return node_all;
    }

    void makeKeywordToFlagsMap(Node * start_node = nullptr)
    {
        if (!start_node)
        {
            start_node = flags_to_keyword_tree.get();
            keyword_to_flags_map["USAGE"] = {};
            keyword_to_flags_map["NONE"] = {};
            keyword_to_flags_map["NO PRIVILEGES"] = {};
        }
        start_node->aliases.emplace_back(start_node->keyword);
        for (auto & alias : start_node->aliases)
        {
            boost::to_upper(alias);
            keyword_to_flags_map[alias] = start_node->flags;
        }
        for (auto & child : start_node->children)
            makeKeywordToFlagsMap(child.get());
    }

    void makeAccessTypeToFlagsMapping()
    {
        access_type_to_flags_mapping.resize(MAX_ACCESS_TYPE);
        for (auto access_type : ext::range_with_static_cast<AccessType>(0, MAX_ACCESS_TYPE))
        {
            auto str = toKeyword(access_type);
            auto it = keyword_to_flags_map.find(str);
            if (it == keyword_to_flags_map.end())
            {
                String uppercased{str};
                boost::to_upper(uppercased);
                it = keyword_to_flags_map.find(uppercased);
            }
            access_type_to_flags_mapping[static_cast<size_t>(access_type)] = it->second;
        }
    }

    void collectAllFlags(const Node * start_node = nullptr)
    {
        if (!start_node)
        {
            start_node = flags_to_keyword_tree.get();
            all_flags = start_node->flags;
        }
        if (start_node->target != UNKNOWN_TARGET)
            all_flags_for_target[start_node->target] |= start_node->flags;
        for (const auto & child : start_node->children)
            collectAllFlags(child.get());
    }

    Impl()
    {
        flags_to_keyword_tree = makeFlagsToKeywordTree();
        makeKeywordToFlagsMap();
        makeAccessTypeToFlagsMapping();
        collectAllFlags();
    }

    std::unique_ptr<Node> flags_to_keyword_tree;
    std::unordered_map<std::string_view, Flags> keyword_to_flags_map;
    std::vector<Flags> access_type_to_flags_mapping;
    Flags all_flags;
    Flags all_flags_for_target[NUM_TARGETS];
};


inline AccessFlags::AccessFlags(AccessType type) : flags(Impl<>::instance().accessTypeToFlags(type)) {}
inline AccessFlags::AccessFlags(const std::string_view & keyword) : flags(Impl<>::instance().keywordToFlags(keyword)) {}
inline AccessFlags::AccessFlags(const std::vector<std::string_view> & keywords) : flags(Impl<>::instance().keywordsToFlags(keywords)) {}
inline AccessFlags::AccessFlags(const Strings & keywords) : flags(Impl<>::instance().keywordsToFlags(keywords)) {}
inline String AccessFlags::toString() const { return Impl<>::instance().flagsToString(flags); }
inline std::vector<std::string_view> AccessFlags::toKeywords() const { return Impl<>::instance().flagsToKeywords(flags); }
inline AccessFlags AccessFlags::allFlags() { return Impl<>::instance().getAllFlags(); }
inline AccessFlags AccessFlags::allGlobalFlags() { return Impl<>::instance().getGlobalFlags(); }
inline AccessFlags AccessFlags::allDatabaseFlags() { return Impl<>::instance().getDatabaseFlags(); }
inline AccessFlags AccessFlags::allTableFlags() { return Impl<>::instance().getTableFlags(); }
inline AccessFlags AccessFlags::allColumnFlags() { return Impl<>::instance().getColumnFlags(); }
inline AccessFlags AccessFlags::allDictionaryFlags() { return Impl<>::instance().getDictionaryFlags(); }

inline AccessFlags operator |(AccessType left, AccessType right) { return AccessFlags(left) | right; }
inline AccessFlags operator &(AccessType left, AccessType right) { return AccessFlags(left) & right; }
inline AccessFlags operator -(AccessType left, AccessType right) { return AccessFlags(left) - right; }
inline AccessFlags operator ~(AccessType x) { return ~AccessFlags(x); }

}
