#pragma once

#include <Access/AccessType.h>
#include <common/types.h>
#include <Common/Exception.h>
#include <ext/range.h>
#include <ext/push_back.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <bitset>
#include <unordered_map>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
    friend bool operator <(const AccessFlags & left, const AccessFlags & right) { return memcmp(&left.flags, &right.flags, sizeof(Flags)) < 0; }
    friend bool operator >(const AccessFlags & left, const AccessFlags & right) { return right < left; }
    friend bool operator <=(const AccessFlags & left, const AccessFlags & right) { return !(right < left); }
    friend bool operator >=(const AccessFlags & left, const AccessFlags & right) { return !(left < right); }

    void clear() { flags.reset(); }

    /// Returns a comma-separated list of keywords, like "SELECT, CREATE USER, UPDATE".
    String toString() const;

    /// Returns a list of access types.
    std::vector<AccessType> toAccessTypes() const;

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

    /// Returns all the flags which could be granted on the global level.
    /// The same as allFlags().
    static AccessFlags allFlagsGrantableOnGlobalLevel();

    /// Returns all the flags which could be granted on the database level.
    /// Returns allDatabaseFlags() | allTableFlags() | allDictionaryFlags() | allColumnFlags().
    static AccessFlags allFlagsGrantableOnDatabaseLevel();

    /// Returns all the flags which could be granted on the table level.
    /// Returns allTableFlags() | allDictionaryFlags() | allColumnFlags().
    static AccessFlags allFlagsGrantableOnTableLevel();

    /// Returns all the flags which could be granted on the global level.
    /// The same as allColumnFlags().
    static AccessFlags allFlagsGrantableOnColumnLevel();

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

    std::vector<AccessType> flagsToAccessTypes(const Flags & flags_) const
    {
        std::vector<AccessType> access_types;
        flagsToAccessTypesRec(flags_, access_types, *all_node);
        return access_types;
    }

    std::vector<std::string_view> flagsToKeywords(const Flags & flags_) const
    {
        std::vector<std::string_view> keywords;
        flagsToKeywordsRec(flags_, keywords, *all_node);
        return keywords;
    }

    String flagsToString(const Flags & flags_) const
    {
        auto keywords = flagsToKeywords(flags_);
        if (keywords.empty())
            return "USAGE";
        String str;
        for (const auto & keyword : keywords)
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
    const Flags & getAllFlagsGrantableOnGlobalLevel() const { return getAllFlags(); }
    const Flags & getAllFlagsGrantableOnDatabaseLevel() const { return all_flags_grantable_on_database_level; }
    const Flags & getAllFlagsGrantableOnTableLevel() const { return all_flags_grantable_on_table_level; }
    const Flags & getAllFlagsGrantableOnColumnLevel() const { return getColumnFlags(); }

private:
    enum NodeType
    {
        UNKNOWN = -2,
        GROUP = -1,
        GLOBAL,
        DATABASE,
        TABLE,
        VIEW = TABLE,
        COLUMN,
        DICTIONARY,
    };

    struct Node;
    using NodePtr = std::unique_ptr<Node>;

    struct Node
    {
        const String keyword;
        NodeType node_type;
        AccessType access_type = AccessType::NONE;
        Strings aliases;
        Flags flags;
        std::vector<NodePtr> children;

        Node(String keyword_, NodeType node_type_ = UNKNOWN) : keyword(std::move(keyword_)), node_type(node_type_) {}

        void setFlag(size_t flag) { flags.set(flag); }

        void addChild(NodePtr child)
        {
            flags |= child->flags;
            children.push_back(std::move(child));
        }
    };

    static String replaceUnderscoreWithSpace(const std::string_view & str)
    {
        String res{str};
        boost::replace_all(res, "_", " ");
        return res;
    }

    static Strings splitAliases(const std::string_view & str)
    {
        Strings aliases;
        boost::split(aliases, str, boost::is_any_of(","));
        for (auto & alias : aliases)
            boost::trim(alias);
        return aliases;
    }

    static void makeNode(
        AccessType access_type,
        const std::string_view & name,
        const std::string_view & aliases,
        NodeType node_type,
        const std::string_view & parent_group_name,
        std::unordered_map<std::string_view, Node *> & nodes,
        std::unordered_map<std::string_view, NodePtr> & owned_nodes,
        size_t & next_flag)
    {
        NodePtr node;
        auto keyword = replaceUnderscoreWithSpace(name);
        auto it = owned_nodes.find(keyword);
        if (it != owned_nodes.end())
        {
            node = std::move(it->second);
            owned_nodes.erase(it);
        }
        else
        {
            if (nodes.count(keyword))
                throw Exception(keyword + " declared twice", ErrorCodes::LOGICAL_ERROR);
            node = std::make_unique<Node>(keyword, node_type);
            nodes[node->keyword] = node.get();
        }

        node->access_type = access_type;
        node->node_type = node_type;
        node->aliases = splitAliases(aliases);
        if (node_type != GROUP)
            node->setFlag(next_flag++);

        bool has_parent_group = (parent_group_name != std::string_view{"NONE"});
        if (!has_parent_group)
        {
            std::string_view keyword_as_string_view = node->keyword;
            owned_nodes[keyword_as_string_view] = std::move(node);
            return;
        }

        auto parent_keyword = replaceUnderscoreWithSpace(parent_group_name);
        auto it_parent = nodes.find(parent_keyword);
        if (it_parent == nodes.end())
        {
            auto parent_node = std::make_unique<Node>(parent_keyword);
            it_parent = nodes.emplace(parent_node->keyword, parent_node.get()).first;
            assert(!owned_nodes.count(parent_node->keyword));
            std::string_view parent_keyword_as_string_view = parent_node->keyword;
            owned_nodes[parent_keyword_as_string_view] = std::move(parent_node);
        }
        it_parent->second->addChild(std::move(node));
    }

    void makeNodes()
    {
        std::unordered_map<std::string_view, NodePtr> owned_nodes;
        std::unordered_map<std::string_view, Node *> nodes;
        size_t next_flag = 0;

#define MAKE_ACCESS_FLAGS_NODE(name, aliases, node_type, parent_group_name) \
            makeNode(AccessType::name, #name, aliases, node_type, #parent_group_name, nodes, owned_nodes, next_flag);

            APPLY_FOR_ACCESS_TYPES(MAKE_ACCESS_FLAGS_NODE)

#undef MAKE_ACCESS_FLAGS_NODE

        if (!owned_nodes.count("NONE"))
            throw Exception("'NONE' not declared", ErrorCodes::LOGICAL_ERROR);
        if (!owned_nodes.count("ALL"))
            throw Exception("'ALL' not declared", ErrorCodes::LOGICAL_ERROR);

        all_node = std::move(owned_nodes["ALL"]);
        none_node = std::move(owned_nodes["NONE"]);
        owned_nodes.erase("ALL");
        owned_nodes.erase("NONE");

        if (!owned_nodes.empty())
        {
            const auto & unused_node = *(owned_nodes.begin()->second);
            if (unused_node.node_type == UNKNOWN)
                throw Exception("Parent group '" + unused_node.keyword + "' not found", ErrorCodes::LOGICAL_ERROR);
            else
                throw Exception("Access type '" + unused_node.keyword + "' should have parent group", ErrorCodes::LOGICAL_ERROR);
        }
    }

    void makeKeywordToFlagsMap(Node * start_node = nullptr)
    {
        if (!start_node)
        {
            makeKeywordToFlagsMap(none_node.get());
            start_node = all_node.get();
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

    void makeAccessTypeToFlagsMapping(Node * start_node = nullptr)
    {
        if (!start_node)
        {
            makeAccessTypeToFlagsMapping(none_node.get());
            start_node = all_node.get();
        }

        size_t index = static_cast<size_t>(start_node->access_type);
        access_type_to_flags_mapping.resize(std::max(index + 1, access_type_to_flags_mapping.size()));
        access_type_to_flags_mapping[index] = start_node->flags;

        for (auto & child : start_node->children)
            makeAccessTypeToFlagsMapping(child.get());
    }

    void collectAllFlags(const Node * start_node = nullptr)
    {
        if (!start_node)
        {
            start_node = all_node.get();
            all_flags = start_node->flags;
        }
        if (start_node->node_type != GROUP)
        {
            assert(static_cast<size_t>(start_node->node_type) < std::size(all_flags_for_target));
            all_flags_for_target[start_node->node_type] |= start_node->flags;
        }
        for (const auto & child : start_node->children)
            collectAllFlags(child.get());

        all_flags_grantable_on_table_level = all_flags_for_target[TABLE] | all_flags_for_target[DICTIONARY] | all_flags_for_target[COLUMN];
        all_flags_grantable_on_database_level = all_flags_for_target[DATABASE] | all_flags_grantable_on_table_level;
    }

    Impl()
    {
        makeNodes();
        makeKeywordToFlagsMap();
        makeAccessTypeToFlagsMapping();
        collectAllFlags();
    }

    static void flagsToAccessTypesRec(const Flags & flags_, std::vector<AccessType> & access_types, const Node & start_node)
    {
        Flags matching_flags = (flags_ & start_node.flags);
        if (matching_flags.any())
        {
            if (matching_flags == start_node.flags)
            {
                access_types.push_back(start_node.access_type);
            }
            else
            {
                for (const auto & child : start_node.children)
                   flagsToAccessTypesRec(flags_, access_types, *child);
            }
        }
    }

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

    NodePtr all_node;
    NodePtr none_node;
    std::unordered_map<std::string_view, Flags> keyword_to_flags_map;
    std::vector<Flags> access_type_to_flags_mapping;
    Flags all_flags;
    Flags all_flags_for_target[static_cast<size_t>(DICTIONARY) + 1];
    Flags all_flags_grantable_on_database_level;
    Flags all_flags_grantable_on_table_level;
};


inline AccessFlags::AccessFlags(AccessType type) : flags(Impl<>::instance().accessTypeToFlags(type)) {}
inline AccessFlags::AccessFlags(const std::string_view & keyword) : flags(Impl<>::instance().keywordToFlags(keyword)) {}
inline AccessFlags::AccessFlags(const std::vector<std::string_view> & keywords) : flags(Impl<>::instance().keywordsToFlags(keywords)) {}
inline AccessFlags::AccessFlags(const Strings & keywords) : flags(Impl<>::instance().keywordsToFlags(keywords)) {}
inline String AccessFlags::toString() const { return Impl<>::instance().flagsToString(flags); }
inline std::vector<AccessType> AccessFlags::toAccessTypes() const { return Impl<>::instance().flagsToAccessTypes(flags); }
inline std::vector<std::string_view> AccessFlags::toKeywords() const { return Impl<>::instance().flagsToKeywords(flags); }
inline AccessFlags AccessFlags::allFlags() { return Impl<>::instance().getAllFlags(); }
inline AccessFlags AccessFlags::allGlobalFlags() { return Impl<>::instance().getGlobalFlags(); }
inline AccessFlags AccessFlags::allDatabaseFlags() { return Impl<>::instance().getDatabaseFlags(); }
inline AccessFlags AccessFlags::allTableFlags() { return Impl<>::instance().getTableFlags(); }
inline AccessFlags AccessFlags::allColumnFlags() { return Impl<>::instance().getColumnFlags(); }
inline AccessFlags AccessFlags::allDictionaryFlags() { return Impl<>::instance().getDictionaryFlags(); }
inline AccessFlags AccessFlags::allFlagsGrantableOnGlobalLevel() { return Impl<>::instance().getAllFlagsGrantableOnGlobalLevel(); }
inline AccessFlags AccessFlags::allFlagsGrantableOnDatabaseLevel() { return Impl<>::instance().getAllFlagsGrantableOnDatabaseLevel(); }
inline AccessFlags AccessFlags::allFlagsGrantableOnTableLevel() { return Impl<>::instance().getAllFlagsGrantableOnTableLevel(); }
inline AccessFlags AccessFlags::allFlagsGrantableOnColumnLevel() { return Impl<>::instance().getAllFlagsGrantableOnColumnLevel(); }

inline AccessFlags operator |(AccessType left, AccessType right) { return AccessFlags(left) | right; }
inline AccessFlags operator &(AccessType left, AccessType right) { return AccessFlags(left) & right; }
inline AccessFlags operator -(AccessType left, AccessType right) { return AccessFlags(left) - right; }
inline AccessFlags operator ~(AccessType x) { return ~AccessFlags(x); }

}
