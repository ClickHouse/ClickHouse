#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessType.h>
#include <Common/Exception.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <unordered_map>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_ACCESS_TYPE;
    extern const int LOGICAL_ERROR;
    extern const int MIXED_ACCESS_PARAMETER_TYPES;
}

namespace
{
    using Flags = std::bitset<AccessFlags::SIZE>;

    class Helper
    {
    public:
        static const Helper & instance()
        {
            static const Helper res;
            return res;
        }

        Flags accessTypeToFlags(AccessType type) const
        {
            return access_type_to_flags_mapping[static_cast<size_t>(type)];
        }

        Flags keywordToFlags(std::string_view keyword) const
        {
            auto it = keyword_to_flags_map.find(keyword);
            if (it == keyword_to_flags_map.end())
            {
                String uppercased_keyword{keyword};
                boost::to_upper(uppercased_keyword);
                it = keyword_to_flags_map.find(uppercased_keyword);
                if (it == keyword_to_flags_map.end())
                    throw Exception(ErrorCodes::UNKNOWN_ACCESS_TYPE, "Unknown access type: {}", String(keyword));
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
        const Flags & getGlobalWithParameterFlags() const { return all_flags_grantable_on_global_with_parameter_level; }
        const Flags & getDatabaseFlags() const { return all_flags_for_target[DATABASE]; }
        const Flags & getTableFlags() const { return all_flags_for_target[TABLE]; }
        const Flags & getColumnFlags() const { return all_flags_for_target[COLUMN]; }
        const Flags & getDictionaryFlags() const { return all_flags_for_target[DICTIONARY]; }
        const Flags & getTableEngineFlags() const { return all_flags_for_target[TABLE_ENGINE]; }
        const Flags & getUserNameFlags() const { return all_flags_for_target[USER_NAME]; }
        const Flags & getNamedCollectionFlags() const { return all_flags_for_target[NAMED_COLLECTION]; }
        const Flags & getAllFlagsGrantableOnGlobalLevel() const { return getAllFlags(); }
        const Flags & getAllFlagsGrantableOnGlobalWithParameterLevel() const { return getGlobalWithParameterFlags(); }
        const Flags & getAllFlagsGrantableOnDatabaseLevel() const { return all_flags_grantable_on_database_level; }
        const Flags & getAllFlagsGrantableOnTableLevel() const { return all_flags_grantable_on_table_level; }
        const Flags & getAllFlagsGrantableOnColumnLevel() const { return getColumnFlags(); }

    private:
        enum NodeType
        {
            UNKNOWN = -2,
            GROUP = -1,
            GLOBAL = 0,
            DATABASE = 1,
            TABLE = 2,
            VIEW = TABLE,
            COLUMN = 3,
            DICTIONARY = 4,
            NAMED_COLLECTION = 5,
            USER_NAME = 6,
            TABLE_ENGINE = 7,
        };

        struct Node;
        using NodePtr = std::unique_ptr<Node>;

        struct Node
        {
            const String keyword;
            NodeType node_type = UNKNOWN;
            AccessType access_type = AccessType::NONE;
            Strings aliases;
            Flags flags;
            std::vector<NodePtr> children;

            explicit Node(String keyword_) : keyword(std::move(keyword_)) {}
            Node(String keyword_, NodeType node_type_) : keyword(std::move(keyword_)), node_type(node_type_) {}

            void setFlag(size_t flag) { flags.set(flag); }

            void addChild(NodePtr child)
            {
                flags |= child->flags;
                children.push_back(std::move(child));
            }
        };

        static String replaceUnderscoreWithSpace(std::string_view str)
        {
            String res{str};
            boost::replace_all(res, "_", " ");
            return res;
        }

        static Strings splitAliases(std::string_view str)
        {
            Strings aliases;
            boost::split(aliases, str, boost::is_any_of(","));
            for (auto & alias : aliases)
                boost::trim(alias);
            return aliases;
        }

        static void makeNode(
            AccessType access_type,
            std::string_view name,
            std::string_view aliases,
            NodeType node_type,
            std::string_view parent_group_name,
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
                if (nodes.contains(keyword))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} declared twice", keyword);
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
                assert(!owned_nodes.contains(parent_node->keyword));
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

#           define MAKE_ACCESS_FLAGS_NODE(name, aliases, node_type, parent_group_name) \
                makeNode(AccessType::name, #name, aliases, node_type, #parent_group_name, nodes, owned_nodes, next_flag);

                APPLY_FOR_ACCESS_TYPES(MAKE_ACCESS_FLAGS_NODE)

#           undef MAKE_ACCESS_FLAGS_NODE

            if (!owned_nodes.contains("NONE"))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "'NONE' not declared");
            if (!owned_nodes.contains("ALL"))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "'ALL' not declared");

            all_node = std::move(owned_nodes["ALL"]);
            none_node = std::move(owned_nodes["NONE"]);
            owned_nodes.erase("ALL");
            owned_nodes.erase("NONE");

            if (!owned_nodes.empty())
            {
                const auto & unused_node = *(owned_nodes.begin()->second);
                if (unused_node.node_type == UNKNOWN)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Parent group '{}' not found", unused_node.keyword);
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Access type '{}' should have parent group", unused_node.keyword);
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
            all_flags_grantable_on_global_with_parameter_level = all_flags_for_target[NAMED_COLLECTION] | all_flags_for_target[USER_NAME] | all_flags_for_target[TABLE_ENGINE];
            all_flags_grantable_on_database_level = all_flags_for_target[DATABASE] | all_flags_grantable_on_table_level;
        }

        Helper()
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
        Flags all_flags_for_target[static_cast<size_t>(TABLE_ENGINE) + 1];
        Flags all_flags_grantable_on_database_level;
        Flags all_flags_grantable_on_table_level;
        Flags all_flags_grantable_on_global_with_parameter_level;
    };
}

bool AccessFlags::isGlobalWithParameter() const
{
    return getParameterType() != AccessFlags::NONE;
}

std::unordered_map<AccessFlags::ParameterType, AccessFlags> AccessFlags::splitIntoParameterTypes() const
{
    std::unordered_map<ParameterType, AccessFlags> result;

    auto named_collection_flags = AccessFlags::allNamedCollectionFlags() & *this;
    if (named_collection_flags)
        result.emplace(ParameterType::NAMED_COLLECTION, named_collection_flags);

    auto user_flags = AccessFlags::allUserNameFlags() & *this;
    if (user_flags)
        result.emplace(ParameterType::USER_NAME, user_flags);

    auto table_engine_flags = AccessFlags::allTableEngineFlags() & *this;
    if (table_engine_flags)
        result.emplace(ParameterType::TABLE_ENGINE, table_engine_flags);

    auto other_flags = (~named_collection_flags & ~user_flags & ~table_engine_flags) & *this;
    if (other_flags)
        result.emplace(ParameterType::NONE, other_flags);

    return result;
}

AccessFlags::ParameterType AccessFlags::getParameterType() const
{
    if (isEmpty() || !AccessFlags::allGlobalWithParameterFlags().contains(*this))
        return AccessFlags::NONE;

    /// All flags refer to NAMED COLLECTION access type.
    if (AccessFlags::allNamedCollectionFlags().contains(*this))
        return AccessFlags::NAMED_COLLECTION;

    if (AccessFlags::allUserNameFlags().contains(*this))
        return AccessFlags::USER_NAME;

    /// All flags refer to TABLE ENGINE access type.
    if (AccessFlags::allTableEngineFlags().contains(*this))
        return AccessFlags::TABLE_ENGINE;

    throw Exception(ErrorCodes::MIXED_ACCESS_PARAMETER_TYPES, "Having mixed parameter types: {}", toString());
}

AccessFlags::AccessFlags(AccessType type) : flags(Helper::instance().accessTypeToFlags(type)) {}
AccessFlags::AccessFlags(std::string_view keyword) : flags(Helper::instance().keywordToFlags(keyword)) {}
AccessFlags::AccessFlags(const std::vector<std::string_view> & keywords) : flags(Helper::instance().keywordsToFlags(keywords)) {}
AccessFlags::AccessFlags(const Strings & keywords) : flags(Helper::instance().keywordsToFlags(keywords)) {}
String AccessFlags::toString() const { return Helper::instance().flagsToString(flags); }
std::vector<AccessType> AccessFlags::toAccessTypes() const { return Helper::instance().flagsToAccessTypes(flags); }
std::vector<std::string_view> AccessFlags::toKeywords() const { return Helper::instance().flagsToKeywords(flags); }
AccessFlags AccessFlags::allFlags() { return Helper::instance().getAllFlags(); }
AccessFlags AccessFlags::allGlobalFlags() { return Helper::instance().getGlobalFlags(); }
AccessFlags AccessFlags::allGlobalWithParameterFlags() { return Helper::instance().getGlobalWithParameterFlags(); }
AccessFlags AccessFlags::allDatabaseFlags() { return Helper::instance().getDatabaseFlags(); }
AccessFlags AccessFlags::allTableFlags() { return Helper::instance().getTableFlags(); }
AccessFlags AccessFlags::allColumnFlags() { return Helper::instance().getColumnFlags(); }
AccessFlags AccessFlags::allDictionaryFlags() { return Helper::instance().getDictionaryFlags(); }
AccessFlags AccessFlags::allNamedCollectionFlags() { return Helper::instance().getNamedCollectionFlags(); }
AccessFlags AccessFlags::allUserNameFlags() { return Helper::instance().getUserNameFlags(); }
AccessFlags AccessFlags::allTableEngineFlags() { return Helper::instance().getTableEngineFlags(); }
AccessFlags AccessFlags::allFlagsGrantableOnGlobalLevel() { return Helper::instance().getAllFlagsGrantableOnGlobalLevel(); }
AccessFlags AccessFlags::allFlagsGrantableOnGlobalWithParameterLevel() { return Helper::instance().getAllFlagsGrantableOnGlobalWithParameterLevel(); }
AccessFlags AccessFlags::allFlagsGrantableOnDatabaseLevel() { return Helper::instance().getAllFlagsGrantableOnDatabaseLevel(); }
AccessFlags AccessFlags::allFlagsGrantableOnTableLevel() { return Helper::instance().getAllFlagsGrantableOnTableLevel(); }
AccessFlags AccessFlags::allFlagsGrantableOnColumnLevel() { return Helper::instance().getAllFlagsGrantableOnColumnLevel(); }

AccessFlags operator |(AccessType left, AccessType right) { return AccessFlags(left) | right; }
AccessFlags operator &(AccessType left, AccessType right) { return AccessFlags(left) & right; }
AccessFlags operator -(AccessType left, AccessType right) { return AccessFlags(left) - right; }
AccessFlags operator ~(AccessType x) { return ~AccessFlags(x); }

}
