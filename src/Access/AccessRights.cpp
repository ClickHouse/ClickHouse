#include <Access/AccessRights.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <boost/container/small_vector.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <unordered_map>

namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_GRANT;
}


namespace
{
    enum Level
    {
        GLOBAL_LEVEL,
        DATABASE_LEVEL,
        TABLE_LEVEL,
        COLUMN_LEVEL,
    };

    struct Helper
    {
        static const Helper & instance()
        {
            static const Helper res;
            return res;
        }

        const AccessFlags all_flags = AccessFlags::allFlags();
        const AccessFlags database_flags = AccessFlags::allDatabaseFlags();
        const AccessFlags table_flags = AccessFlags::allTableFlags();
        const AccessFlags column_flags = AccessFlags::allColumnFlags();
        const AccessFlags dictionary_flags = AccessFlags::allDictionaryFlags();
        const AccessFlags column_level_flags = column_flags;
        const AccessFlags table_level_flags = table_flags | dictionary_flags | column_level_flags;
        const AccessFlags database_level_flags = database_flags | table_level_flags;

        const AccessFlags show_databases_flag = AccessType::SHOW_DATABASES;
        const AccessFlags show_tables_flag = AccessType::SHOW_TABLES;
        const AccessFlags show_columns_flag = AccessType::SHOW_COLUMNS;
        const AccessFlags show_dictionaries_flag = AccessType::SHOW_DICTIONARIES;
        const AccessFlags create_table_flag = AccessType::CREATE_TABLE;
        const AccessFlags create_view_flag = AccessType::CREATE_VIEW;
        const AccessFlags create_temporary_table_flag = AccessType::CREATE_TEMPORARY_TABLE;
        const AccessFlags alter_table_flag = AccessType::ALTER_TABLE;
        const AccessFlags alter_view_flag = AccessType::ALTER_VIEW;
        const AccessFlags truncate_flag = AccessType::TRUNCATE;
        const AccessFlags drop_table_flag = AccessType::DROP_TABLE;
        const AccessFlags drop_view_flag = AccessType::DROP_VIEW;
        const AccessFlags alter_ttl_flag = AccessType::ALTER_TTL;
        const AccessFlags alter_materialize_ttl_flag = AccessType::ALTER_MATERIALIZE_TTL;
        const AccessFlags system_reload_dictionary = AccessType::SYSTEM_RELOAD_DICTIONARY;
        const AccessFlags system_reload_embedded_dictionaries = AccessType::SYSTEM_RELOAD_EMBEDDED_DICTIONARIES;
    };

    using Kind = AccessRightsElementWithOptions::Kind;

    struct ProtoElement
    {
        AccessFlags access_flags;
        boost::container::small_vector<std::string_view, 3> full_name;
        bool grant_option = false;
        Kind kind = Kind::GRANT;

        friend bool operator<(const ProtoElement & left, const ProtoElement & right)
        {
            static constexpr auto compare_name = [](const boost::container::small_vector<std::string_view, 3> & left_name,
                                                    const boost::container::small_vector<std::string_view, 3> & right_name,
                                                    size_t i)
            {
                if (i < left_name.size())
                {
                    if (i < right_name.size())
                        return left_name[i].compare(right_name[i]);
                    else
                        return 1; /// left_name is longer => left_name > right_name
                }
                else if (i < right_name.size())
                    return 1; /// right_name is longer => left < right
                else
                    return 0; /// left_name == right_name
            };

            if (int cmp = compare_name(left.full_name, right.full_name, 0))
                return cmp < 0;

            if (int cmp = compare_name(left.full_name, right.full_name, 1))
                return cmp < 0;

            if (left.kind != right.kind)
                return (left.kind == Kind::GRANT);

            if (left.grant_option != right.grant_option)
                return right.grant_option;

            if (int cmp = compare_name(left.full_name, right.full_name, 2))
                return cmp < 0;

            return (left.access_flags < right.access_flags);
        }

        AccessRightsElementWithOptions getResult() const
        {
            AccessRightsElementWithOptions res;
            res.access_flags = access_flags;
            res.grant_option = grant_option;
            res.kind = kind;
            switch (full_name.size())
            {
                case 0:
                {
                    res.any_database = true;
                    res.any_table = true;
                    res.any_column = true;
                    break;
                }
                case 1:
                {
                    res.any_database = false;
                    res.database = full_name[0];
                    res.any_table = true;
                    res.any_column = true;
                    break;
                }
                case 2:
                {
                    res.any_database = false;
                    res.database = full_name[0];
                    res.any_table = false;
                    res.table = full_name[1];
                    res.any_column = true;
                    break;
                }
                case 3:
                {
                    res.any_database = false;
                    res.database = full_name[0];
                    res.any_table = false;
                    res.table = full_name[1];
                    res.any_column = false;
                    res.columns.emplace_back(full_name[2]);
                    break;
                }
            }
            return res;
        }
    };

    class ProtoElements : public std::vector<ProtoElement>
    {
    public:
        AccessRightsElementsWithOptions getResult() const
        {
            ProtoElements sorted = *this;
            boost::range::sort(sorted);
            AccessRightsElementsWithOptions res;
            res.reserve(sorted.size());

            for (size_t i = 0; i != sorted.size();)
            {
                size_t count_elements_with_diff_columns = sorted.countElementsWithDifferenceInColumnOnly(i);
                if (count_elements_with_diff_columns == 1)
                {
                    /// Easy case: one Element is converted to one AccessRightsElement.
                    const auto & element = sorted[i];
                    if (element.access_flags)
                        res.emplace_back(element.getResult());
                    ++i;
                }
                else
                {
                    /// Difficult case: multiple Elements are converted to one or multiple AccessRightsElements.
                    sorted.appendResultWithElementsWithDifferenceInColumnOnly(i, count_elements_with_diff_columns, res);
                    i += count_elements_with_diff_columns;
                }
            }
            return res;
        }

    private:
        size_t countElementsWithDifferenceInColumnOnly(size_t start) const
        {
            const auto & start_element = (*this)[start];
            if ((start_element.full_name.size() != 3) || (start == size() - 1))
                return 1;

            auto it = std::find_if(begin() + start + 1, end(), [&](const ProtoElement & element)
            {
                return (element.full_name.size() != 3) || (element.full_name[0] != start_element.full_name[0])
                    || (element.full_name[1] != start_element.full_name[1]) || (element.grant_option != start_element.grant_option)
                    || (element.kind != start_element.kind);
            });

            return it - (begin() + start);
        }

        /// Collects columns together to write multiple columns into one AccessRightsElement.
        /// That procedure allows to output access rights in more compact way,
        /// e.g. "SELECT(x, y)" instead of "SELECT(x), SELECT(y)".
        void appendResultWithElementsWithDifferenceInColumnOnly(size_t start, size_t count, AccessRightsElementsWithOptions & res) const
        {
            const auto * pbegin = data() + start;
            const auto * pend = pbegin + count;
            AccessFlags handled_flags;

            while (pbegin < pend)
            {
                while (pbegin < pend && !(pbegin->access_flags - handled_flags))
                    ++pbegin;

                while (pbegin < pend && !((pend - 1)->access_flags - handled_flags))
                    --pend;

                if (pbegin >= pend)
                    break;

                AccessFlags common_flags = (pbegin->access_flags - handled_flags);
                for (const auto * element = pbegin + 1; element != pend; ++element)
                {
                    if (auto new_common_flags = (element->access_flags - handled_flags) & common_flags)
                        common_flags = new_common_flags;
                }

                res.emplace_back();
                auto & back = res.back();
                back.grant_option = pbegin->grant_option;
                back.kind = pbegin->kind;
                back.any_database = false;
                back.database = pbegin->full_name[0];
                back.any_table = false;
                back.table = pbegin->full_name[1];
                back.any_column = false;
                back.access_flags = common_flags;
                for (const auto * element = pbegin; element != pend; ++element)
                {
                    if (((element->access_flags - handled_flags) & common_flags) == common_flags)
                        back.columns.emplace_back(element->full_name[2]);
                }

                handled_flags |= common_flags;
            }
        }
    };
}


struct AccessRights::Node
{
public:
    std::shared_ptr<const String> node_name;
    Level level = GLOBAL_LEVEL;
    AccessFlags access;           /// access = (inherited_access - partial_revokes) | explicit_grants
    AccessFlags final_access;     /// final_access = access | implicit_access
    AccessFlags min_access;       /// min_access = final_access & child[0].final_access & ... & child[N-1].final_access
    AccessFlags max_access;       /// max_access = final_access | child[0].final_access | ... | child[N-1].final_access
    std::unique_ptr<std::unordered_map<std::string_view, Node>> children;

    Node() = default;
    Node(const Node & src) { *this = src; }

    Node & operator =(const Node & src)
    {
        if (this == &src)
            return *this;

        node_name = src.node_name;
        level = src.level;
        access = src.access;
        final_access = src.final_access;
        min_access = src.min_access;
        max_access = src.max_access;
        if (src.children)
            children = std::make_unique<std::unordered_map<std::string_view, Node>>(*src.children);
        else
            children = nullptr;
        return *this;
    }

    void grant(AccessFlags flags, const Helper & helper)
    {
        if (!flags)
            return;

        if (level == GLOBAL_LEVEL)
        {
            /// Everything can be granted on the global level.
        }
        else if (level == DATABASE_LEVEL)
        {
            AccessFlags grantable = flags & helper.database_level_flags;
            if (!grantable)
                throw Exception(flags.toString() + " cannot be granted on the database level", ErrorCodes::INVALID_GRANT);
            flags = grantable;
        }
        else if (level == TABLE_LEVEL)
        {
            AccessFlags grantable = flags & helper.table_level_flags;
            if (!grantable)
                throw Exception(flags.toString() + " cannot be granted on the table level", ErrorCodes::INVALID_GRANT);
            flags = grantable;
        }
        else if (level == COLUMN_LEVEL)
        {
            AccessFlags grantable = flags & helper.column_level_flags;
            if (!grantable)
                throw Exception(flags.toString() + " cannot be granted on the column level", ErrorCodes::INVALID_GRANT);
            flags = grantable;
        }

        addGrantsRec(flags);
        calculateFinalAccessRec(helper);
    }

    template <typename ... Args>
    void grant(const AccessFlags & flags, const Helper & helper, const std::string_view & name, const Args &... subnames)
    {
        auto & child = getChild(name);
        child.grant(flags, helper, subnames...);
        eraseChildIfPossible(child);
        calculateFinalAccess(helper);
    }

    template <typename StringT>
    void grant(const AccessFlags & flags, const Helper & helper, const std::vector<StringT> & names)
    {
        for (const auto & name : names)
        {
            auto & child = getChild(name);
            child.grant(flags, helper);
            eraseChildIfPossible(child);
        }
        calculateFinalAccess(helper);
    }

    void revoke(const AccessFlags & flags, const Helper & helper)
    {
        removeGrantsRec(flags);
        calculateFinalAccessRec(helper);
    }

    template <typename... Args>
    void revoke(const AccessFlags & flags, const Helper & helper, const std::string_view & name, const Args &... subnames)
    {
        auto & child = getChild(name);

        child.revoke(flags, helper, subnames...);
        eraseChildIfPossible(child);
        calculateFinalAccess(helper);
    }

    template <typename StringT>
    void revoke(const AccessFlags & flags, const Helper & helper, const std::vector<StringT> & names)
    {
        for (const auto & name : names)
        {
            auto & child = getChild(name);
            child.revoke(flags, helper);
            eraseChildIfPossible(child);
        }
        calculateFinalAccess(helper);
    }

    bool isGranted(const AccessFlags & flags) const
    {
        return min_access.contains(flags);
    }

    template <typename... Args>
    bool isGranted(AccessFlags flags, const std::string_view & name, const Args &... subnames) const
    {
        if (min_access.contains(flags))
            return true;
        if (!max_access.contains(flags))
            return false;

        const Node * child = tryGetChild(name);
        if (child)
            return child->isGranted(flags, subnames...);
        else
            return final_access.contains(flags);
    }

    template <typename StringT>
    bool isGranted(AccessFlags flags, const std::vector<StringT> & names) const
    {
        if (min_access.contains(flags))
            return true;
        if (!max_access.contains(flags))
            return false;

        for (const auto & name : names)
        {
            const Node * child = tryGetChild(name);
            if (child)
            {
                if (!child->isGranted(flags, name))
                    return false;
            }
            else
            {
                if (!final_access.contains(flags))
                    return false;
            }
        }
        return true;
    }

    friend bool operator ==(const Node & left, const Node & right)
    {
        if (left.access != right.access)
            return false;

        if (!left.children)
            return !right.children;

        if (!right.children)
            return false;
        return *left.children == *right.children;
    }

    friend bool operator!=(const Node & left, const Node & right) { return !(left == right); }

    void merge(const Node & other, const Helper & helper)
    {
        mergeAccessRec(other);
        calculateFinalAccessRec(helper);
    }


    ProtoElements getElements() const
    {
        ProtoElements res;
        getElementsRec(res, {}, *this, {});
        return res;
    }

    static ProtoElements getElements(const Node * node, const Node * node_with_grant_option)
    {
        ProtoElements res;
        getElementsRec(res, {}, node, {}, node_with_grant_option, {});
        return res;
    }

    void logTree(Poco::Logger * log, const String & title) const
    {
        LOG_TRACE(log, "Tree({}): level={}, name={}, access={}, final_access={}, min_access={}, max_access={}, num_children={}",
            title, level, node_name ? *node_name : "NULL", access.toString(),
            final_access.toString(), min_access.toString(), max_access.toString(),
            (children ? children->size() : 0));

        if (children)
        {
            for (auto & child : *children | boost::adaptors::map_values)
                child.logTree(log, title);
        }
    }

private:
    Node * tryGetChild(const std::string_view & name) const
    {
        if (!children)
            return nullptr;
        auto it = children->find(name);
        if (it == children->end())
            return nullptr;
        return &it->second;
    }

    Node & getChild(const std::string_view & name)
    {
        auto * child = tryGetChild(name);
        if (child)
            return *child;
        if (!children)
            children = std::make_unique<std::unordered_map<std::string_view, Node>>();
        auto new_child_name = std::make_shared<const String>(name);
        Node & new_child = (*children)[*new_child_name];
        new_child.node_name = std::move(new_child_name);
        new_child.level = static_cast<Level>(level + 1);
        new_child.access = access;
        return new_child;
    }

    void eraseChildIfPossible(Node & child)
    {
        if (!canEraseChild(child))
            return;
        auto it = children->find(*child.node_name);
        children->erase(it);
        if (children->empty())
            children = nullptr;
    }

    bool canEraseChild(const Node & child) const
    {
        return (access == child.access) && !child.children;
    }

    void addGrantsRec(const AccessFlags & flags)
    {
        access |= flags;
        if (children)
        {
            for (auto it = children->begin(); it != children->end();)
            {
                auto & child = it->second;
                child.addGrantsRec(flags);
                if (canEraseChild(child))
                    it = children->erase(it);
                else
                    ++it;
            }
            if (children->empty())
                children = nullptr;
        }
    }

    void removeGrantsRec(const AccessFlags & flags)
    {
        access &= ~flags;
        if (children)
        {
            for (auto it = children->begin(); it != children->end();)
            {
                auto & child = it->second;
                child.removeGrantsRec(flags);
                if (canEraseChild(child))
                    it = children->erase(it);
                else
                    ++it;
            }
            if (children->empty())
                children = nullptr;
        }
    }

    static void getElementsRec(
        ProtoElements & res,
        const boost::container::small_vector<std::string_view, 3> & full_name,
        const Node & node,
        const AccessFlags & parent_access)
    {
        auto access = node.access;
        auto revokes = parent_access - access;
        auto grants = access - parent_access;

        if (revokes)
            res.push_back(ProtoElement{revokes, full_name, false, Kind::REVOKE});

        if (grants)
            res.push_back(ProtoElement{grants, full_name, false, Kind::GRANT});

        if (node.children)
        {
            for (const auto & [child_name, child] : *node.children)
            {
                boost::container::small_vector<std::string_view, 3> child_full_name = full_name;
                child_full_name.push_back(child_name);
                getElementsRec(res, child_full_name, child, access);
            }
        }
    }

    static void getElementsRec(
        ProtoElements & res,
        const boost::container::small_vector<std::string_view, 3> & full_name,
        const Node * node,
        const AccessFlags & parent_access,
        const Node * node_go,
        const AccessFlags & parent_access_go)
    {
        auto access = node ? node->access : parent_access;
        auto access_go = node_go ? node_go->access : parent_access_go;
        auto revokes = parent_access - access;
        auto revokes_go = parent_access_go - access_go - revokes;
        auto grants_go = access_go - parent_access_go;
        auto grants = access - parent_access - grants_go;

        if (revokes)
            res.push_back(ProtoElement{revokes, full_name, false, Kind::REVOKE});

        if (revokes_go)
            res.push_back(ProtoElement{revokes_go, full_name, true, Kind::REVOKE});

        if (grants)
            res.push_back(ProtoElement{grants, full_name, false, Kind::GRANT});

        if (grants_go)
            res.push_back(ProtoElement{grants_go, full_name, true, Kind::GRANT});

        if (node && node->children)
        {
            for (const auto & [child_name, child] : *node->children)
            {
                boost::container::small_vector<std::string_view, 3> child_full_name = full_name;
                child_full_name.push_back(child_name);
                const Node * child_node = &child;
                const Node * child_node_go = nullptr;
                if (node_go && node_go->children)
                {
                    auto it = node_go->children->find(child_name);
                    if (it != node_go->children->end())
                        child_node_go = &it->second;
                }
                getElementsRec(res, child_full_name, child_node, access, child_node_go, access_go);
            }

        }
        if (node_go && node_go->children)
        {
            for (const auto & [child_name, child] : *node_go->children)
            {
                if (node && node->children && node->children->count(child_name))
                    continue; /// already processed
                boost::container::small_vector<std::string_view, 3> child_full_name = full_name;
                child_full_name.push_back(child_name);
                const Node * child_node = nullptr;
                const Node * child_node_go = &child;
                getElementsRec(res, child_full_name, child_node, access, child_node_go, access_go);
            }
        }
    }

    void calculateFinalAccessRec(const Helper & helper)
    {
        /// Traverse tree.
        if (children)
        {
            for (auto it = children->begin(); it != children->end();)
            {
                auto & child = it->second;
                child.calculateFinalAccessRec(helper);
                if (canEraseChild(child))
                    it = children->erase(it);
                else
                    ++it;
            }
            if (children->empty())
                children = nullptr;
        }

        calculateFinalAccess(helper);
    }

    void calculateFinalAccess(const Helper & helper)
    {
        /// Calculate min and max access among children.
        AccessFlags min_access_among_children = helper.all_flags;
        AccessFlags max_access_among_children;
        if (children)
        {
            for (const auto & child : *children | boost::adaptors::map_values)
            {
                min_access_among_children &= child.min_access;
                max_access_among_children |= child.max_access;
            }
        }

        /// Calculate implicit access:
        AccessFlags implicit_access;

        if (level <= DATABASE_LEVEL)
        {
            if (access & helper.database_flags)
                implicit_access |= helper.show_databases_flag;
        }
        if (level <= TABLE_LEVEL)
        {
            if (access & helper.table_flags)
                implicit_access |= helper.show_tables_flag;
            if (access & helper.dictionary_flags)
                implicit_access |= helper.show_dictionaries_flag;
        }
        if (level <= COLUMN_LEVEL)
        {
            if (access & helper.column_flags)
                implicit_access |= helper.show_columns_flag;
        }
        if (children && max_access_among_children)
        {
            if (level == DATABASE_LEVEL)
                implicit_access |= helper.show_databases_flag;
            else if (level == TABLE_LEVEL)
                implicit_access |= helper.show_tables_flag;
        }

        if (level == GLOBAL_LEVEL)
        {
            if ((access | max_access_among_children) & helper.create_table_flag)
                implicit_access |= helper.create_temporary_table_flag;

            if (access & helper.system_reload_dictionary)
                implicit_access |= helper.system_reload_embedded_dictionaries;
        }

        if (level <= TABLE_LEVEL)
        {
            if (access & helper.create_table_flag)
                implicit_access |= helper.create_view_flag;

            if (access & helper.drop_table_flag)
                implicit_access |= helper.drop_view_flag;

            if (access & helper.alter_table_flag)
                implicit_access |= helper.alter_view_flag;

            if (access & helper.alter_ttl_flag)
                implicit_access |= helper.alter_materialize_ttl_flag;
        }

        final_access = access | implicit_access;

        /// Calculate min and max access:
        /// min_access = final_access & child[0].final_access & ... & child[N-1].final_access
        /// max_access = final_access | child[0].final_access | ... | child[N-1].final_access
        min_access = final_access & min_access_among_children;
        max_access = final_access | max_access_among_children;
    }

    void mergeAccessRec(const Node & rhs)
    {
        if (rhs.children)
        {
            for (const auto & [rhs_childname, rhs_child] : *rhs.children)
                getChild(rhs_childname).mergeAccessRec(rhs_child);
        }
        access |= rhs.access;
        if (children)
        {
            for (auto & [lhs_childname, lhs_child] : *children)
            {
                if (!rhs.tryGetChild(lhs_childname))
                    lhs_child.access |= rhs.access;
            }
        }
    }
};


AccessRights::AccessRights() = default;
AccessRights::~AccessRights() = default;
AccessRights::AccessRights(AccessRights && src) = default;
AccessRights & AccessRights::operator =(AccessRights && src) = default;


AccessRights::AccessRights(const AccessRights & src)
{
    *this = src;
}


AccessRights & AccessRights::operator =(const AccessRights & src)
{
    if (src.root)
        root = std::make_unique<Node>(*src.root);
    else
        root = nullptr;
    if (src.root_with_grant_option)
        root_with_grant_option = std::make_unique<Node>(*src.root_with_grant_option);
    else
        root_with_grant_option = nullptr;
    return *this;
}


AccessRights::AccessRights(const AccessFlags & access)
{
    grant(access);
}


bool AccessRights::isEmpty() const
{
    return !root && !root_with_grant_option;
}


void AccessRights::clear()
{
    root = nullptr;
    root_with_grant_option = nullptr;
}


template <bool with_grant_option, typename... Args>
void AccessRights::grantImpl(const AccessFlags & flags, const Args &... args)
{
    auto helper = [&](std::unique_ptr<Node> & root_node)
    {
        if (!root_node)
            root_node = std::make_unique<Node>();
        root_node->grant(flags, Helper::instance(), args...);
        if (!root_node->access && !root_node->children)
            root_node = nullptr;
    };
    helper(root);

    if constexpr (with_grant_option)
        helper(root_with_grant_option);
}

template <bool with_grant_option>
void AccessRights::grantImpl(const AccessRightsElement & element)
{
    if (element.any_database)
        grantImpl<with_grant_option>(element.access_flags);
    else if (element.any_table)
        grantImpl<with_grant_option>(element.access_flags, element.database);
    else if (element.any_column)
        grantImpl<with_grant_option>(element.access_flags, element.database, element.table);
    else
        grantImpl<with_grant_option>(element.access_flags, element.database, element.table, element.columns);
}

template <bool with_grant_option>
void AccessRights::grantImpl(const AccessRightsElements & elements)
{
    for (const auto & element : elements)
        grantImpl<with_grant_option>(element);
}

void AccessRights::grant(const AccessFlags & flags) { grantImpl<false>(flags); }
void AccessRights::grant(const AccessFlags & flags, const std::string_view & database) { grantImpl<false>(flags, database); }
void AccessRights::grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) { grantImpl<false>(flags, database, table); }
void AccessRights::grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) { grantImpl<false>(flags, database, table, column); }
void AccessRights::grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { grantImpl<false>(flags, database, table, columns); }
void AccessRights::grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) { grantImpl<false>(flags, database, table, columns); }
void AccessRights::grant(const AccessRightsElement & element) { grantImpl<false>(element); }
void AccessRights::grant(const AccessRightsElements & elements) { grantImpl<false>(elements); }

void AccessRights::grantWithGrantOption(const AccessFlags & flags) { grantImpl<true>(flags); }
void AccessRights::grantWithGrantOption(const AccessFlags & flags, const std::string_view & database) { grantImpl<true>(flags, database); }
void AccessRights::grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) { grantImpl<true>(flags, database, table); }
void AccessRights::grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) { grantImpl<true>(flags, database, table, column); }
void AccessRights::grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { grantImpl<true>(flags, database, table, columns); }
void AccessRights::grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) { grantImpl<true>(flags, database, table, columns); }
void AccessRights::grantWithGrantOption(const AccessRightsElement & element) { grantImpl<true>(element); }
void AccessRights::grantWithGrantOption(const AccessRightsElements & elements) { grantImpl<true>(elements); }


template <bool grant_option, typename... Args>
void AccessRights::revokeImpl(const AccessFlags & flags, const Args &... args)
{
    auto helper = [&](std::unique_ptr<Node> & root_node)
    {
        if (!root_node)
            return;
        root_node->revoke(flags, Helper::instance(), args...);
        if (!root_node->access && !root_node->children)
            root_node = nullptr;
    };
    helper(root_with_grant_option);

    if constexpr (!grant_option)
        helper(root);
}

template <bool grant_option>
void AccessRights::revokeImpl(const AccessRightsElement & element)
{
    if (element.any_database)
        revokeImpl<grant_option>(element.access_flags);
    else if (element.any_table)
        revokeImpl<grant_option>(element.access_flags, element.database);
    else if (element.any_column)
        revokeImpl<grant_option>(element.access_flags, element.database, element.table);
    else
        revokeImpl<grant_option>(element.access_flags, element.database, element.table, element.columns);
}

template <bool grant_option>
void AccessRights::revokeImpl(const AccessRightsElements & elements)
{
    for (const auto & element : elements)
        revokeImpl<grant_option>(element);
}

void AccessRights::revoke(const AccessFlags & flags) { revokeImpl<false>(flags); }
void AccessRights::revoke(const AccessFlags & flags, const std::string_view & database) { revokeImpl<false>(flags, database); }
void AccessRights::revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) { revokeImpl<false>(flags, database, table); }
void AccessRights::revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) { revokeImpl<false>(flags, database, table, column); }
void AccessRights::revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { revokeImpl<false>(flags, database, table, columns); }
void AccessRights::revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) { revokeImpl<false>(flags, database, table, columns); }
void AccessRights::revoke(const AccessRightsElement & element) { revokeImpl<false>(element); }
void AccessRights::revoke(const AccessRightsElements & elements) { revokeImpl<false>(elements); }

void AccessRights::revokeGrantOption(const AccessFlags & flags) { revokeImpl<true>(flags); }
void AccessRights::revokeGrantOption(const AccessFlags & flags, const std::string_view & database) { revokeImpl<true>(flags, database); }
void AccessRights::revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) { revokeImpl<true>(flags, database, table); }
void AccessRights::revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) { revokeImpl<true>(flags, database, table, column); }
void AccessRights::revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { revokeImpl<true>(flags, database, table, columns); }
void AccessRights::revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) { revokeImpl<true>(flags, database, table, columns); }
void AccessRights::revokeGrantOption(const AccessRightsElement & element) { revokeImpl<true>(element); }
void AccessRights::revokeGrantOption(const AccessRightsElements & elements) { revokeImpl<true>(elements); }


AccessRightsElementsWithOptions AccessRights::getElements() const
{
#if 0
    logTree();
#endif
    if (!root)
        return {};
    if (!root_with_grant_option)
        return root->getElements().getResult();
    return Node::getElements(root.get(), root_with_grant_option.get()).getResult();
}


String AccessRights::toString() const
{
    return getElements().toString();
}


template <bool grant_option, typename... Args>
bool AccessRights::isGrantedImpl(const AccessFlags & flags, const Args &... args) const
{
    auto helper = [&](const std::unique_ptr<Node> & root_node) -> bool
    {
        if (!root_node)
            return flags.isEmpty();
        return root_node->isGranted(flags, args...);
    };
    if constexpr (grant_option)
        return helper(root_with_grant_option);
    else
        return helper(root);
}

template <bool grant_option>
bool AccessRights::isGrantedImpl(const AccessRightsElement & element) const
{
    if (element.any_database)
        return isGrantedImpl<grant_option>(element.access_flags);
    else if (element.any_table)
        return isGrantedImpl<grant_option>(element.access_flags, element.database);
    else if (element.any_column)
        return isGrantedImpl<grant_option>(element.access_flags, element.database, element.table);
    else
        return isGrantedImpl<grant_option>(element.access_flags, element.database, element.table, element.columns);
}

template <bool grant_option>
bool AccessRights::isGrantedImpl(const AccessRightsElements & elements) const
{
    for (const auto & element : elements)
        if (!isGrantedImpl<grant_option>(element))
            return false;
    return true;
}

bool AccessRights::isGranted(const AccessFlags & flags) const { return isGrantedImpl<false>(flags); }
bool AccessRights::isGranted(const AccessFlags & flags, const std::string_view & database) const { return isGrantedImpl<false>(flags, database); }
bool AccessRights::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { return isGrantedImpl<false>(flags, database, table); }
bool AccessRights::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return isGrantedImpl<false>(flags, database, table, column); }
bool AccessRights::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return isGrantedImpl<false>(flags, database, table, columns); }
bool AccessRights::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return isGrantedImpl<false>(flags, database, table, columns); }
bool AccessRights::isGranted(const AccessRightsElement & element) const { return isGrantedImpl<false>(element); }
bool AccessRights::isGranted(const AccessRightsElements & elements) const { return isGrantedImpl<false>(elements); }

bool AccessRights::hasGrantOption(const AccessFlags & flags) const { return isGrantedImpl<true>(flags); }
bool AccessRights::hasGrantOption(const AccessFlags & flags, const std::string_view & database) const { return isGrantedImpl<true>(flags, database); }
bool AccessRights::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { return isGrantedImpl<true>(flags, database, table); }
bool AccessRights::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return isGrantedImpl<true>(flags, database, table, column); }
bool AccessRights::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return isGrantedImpl<true>(flags, database, table, columns); }
bool AccessRights::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return isGrantedImpl<true>(flags, database, table, columns); }
bool AccessRights::hasGrantOption(const AccessRightsElement & element) const { return isGrantedImpl<true>(element); }
bool AccessRights::hasGrantOption(const AccessRightsElements & elements) const { return isGrantedImpl<true>(elements); }


bool operator ==(const AccessRights & left, const AccessRights & right)
{
    auto helper = [](const std::unique_ptr<AccessRights::Node> & left_node, const std::unique_ptr<AccessRights::Node> & right_node)
    {
        if (!left_node)
            return !right_node;
        if (!right_node)
            return false;
        return *left_node == *right_node;
    };
    return helper(left.root, right.root) && helper(left.root_with_grant_option, right.root_with_grant_option);
}


void AccessRights::merge(const AccessRights & other)
{
    auto helper = [](std::unique_ptr<Node> & root_node, const std::unique_ptr<Node> & other_root_node)
    {
        if (!root_node)
        {
            if (other_root_node)
                root_node = std::make_unique<Node>(*other_root_node);
            return;
        }
        if (other_root_node)
        {
            root_node->merge(*other_root_node, Helper::instance());
            if (!root_node->access && !root_node->children)
                root_node = nullptr;
        }
    };
    helper(root, other.root);
    helper(root_with_grant_option, other.root_with_grant_option);
}


AccessRights AccessRights::getFullAccess()
{
    AccessRights res;
    res.grantWithGrantOption(AccessType::ALL);
    return res;
}


void AccessRights::logTree() const
{
    auto * log = &Poco::Logger::get("AccessRights");
    if (root)
    {
        root->logTree(log, "");
        if (root_with_grant_option)
            root->logTree(log, "go");
    }
    else
        LOG_TRACE(log, "Tree: NULL");
}
}
