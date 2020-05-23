#include <Access/AccessRights.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <boost/range/adaptor/map.hpp>
#include <unordered_map>

namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_GRANT;
    extern const int LOGICAL_ERROR;
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

    std::string_view checkCurrentDatabase(const std::string_view & current_database)
    {
        if (current_database.empty())
            throw Exception("No current database", ErrorCodes::LOGICAL_ERROR);
        return current_database;
    }
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

    void logTree(Poco::Logger * log) const
    {
        LOG_TRACE_FORMATTED(log, "Tree({}): name={}, access={}, final_access={}, min_access={}, max_access={}, num_children={}",
            level, node_name ? *node_name : "NULL", access.toString(),
            final_access.toString(), min_access.toString(), max_access.toString(),
            (children ? children->size() : 0));

        if (children)
        {
            for (auto & child : *children | boost::adaptors::map_values)
                child.logTree(log);
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
    return *this;
}


AccessRights::AccessRights(const AccessFlags & access)
{
    grant(access);
}


bool AccessRights::isEmpty() const
{
    return !root;
}


void AccessRights::clear()
{
    root = nullptr;
}


template <typename... Args>
void AccessRights::grantImpl(const AccessFlags & flags, const Args &... args)
{
    if (!root)
        root = std::make_unique<Node>();
    root->grant(flags, Helper::instance(), args...);
    if (!root->access && !root->children)
        root = nullptr;
}

void AccessRights::grant(const AccessFlags & flags) { grantImpl(flags); }
void AccessRights::grant(const AccessFlags & flags, const std::string_view & database) { grantImpl(flags, database); }
void AccessRights::grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) { grantImpl(flags, database, table); }
void AccessRights::grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) { grantImpl(flags, database, table, column); }
void AccessRights::grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { grantImpl(flags, database, table, columns); }
void AccessRights::grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) { grantImpl(flags, database, table, columns); }

void AccessRights::grant(const AccessRightsElement & element, std::string_view current_database)
{
    if (element.any_database)
    {
        grant(element.access_flags);
    }
    else if (element.any_table)
    {
        if (element.database.empty())
            grant(element.access_flags, checkCurrentDatabase(current_database));
        else
            grant(element.access_flags, element.database);
    }
    else if (element.any_column)
    {
        if (element.database.empty())
            grant(element.access_flags, checkCurrentDatabase(current_database), element.table);
        else
            grant(element.access_flags, element.database, element.table);
    }
    else
    {
        if (element.database.empty())
            grant(element.access_flags, checkCurrentDatabase(current_database), element.table, element.columns);
        else
            grant(element.access_flags, element.database, element.table, element.columns);
    }
}

void AccessRights::grant(const AccessRightsElements & elements, std::string_view current_database)
{
    for (const auto & element : elements)
        grant(element, current_database);
}


template <typename... Args>
void AccessRights::revokeImpl(const AccessFlags & flags, const Args &... args)
{
    if (!root)
        return;
    root->revoke(flags, Helper::instance(), args...);
    if (!root->access && !root->children)
        root = nullptr;
}

void AccessRights::revoke(const AccessFlags & flags) { revokeImpl(flags); }
void AccessRights::revoke(const AccessFlags & flags, const std::string_view & database) { revokeImpl(flags, database); }
void AccessRights::revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) { revokeImpl(flags, database, table); }
void AccessRights::revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) { revokeImpl(flags, database, table, column); }
void AccessRights::revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { revokeImpl(flags, database, table, columns); }
void AccessRights::revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) { revokeImpl(flags, database, table, columns); }


void AccessRights::revoke(const AccessRightsElement & element, std::string_view current_database)
{
    if (element.any_database)
    {
        revoke(element.access_flags);
    }
    else if (element.any_table)
    {
        if (element.database.empty())
            revoke(element.access_flags, checkCurrentDatabase(current_database));
        else
            revoke(element.access_flags, element.database);
    }
    else if (element.any_column)
    {
        if (element.database.empty())
            revoke(element.access_flags, checkCurrentDatabase(current_database), element.table);
        else
            revoke(element.access_flags, element.database, element.table);
    }
    else
    {
        if (element.database.empty())
            revoke(element.access_flags, checkCurrentDatabase(current_database), element.table, element.columns);
        else
            revoke(element.access_flags, element.database, element.table, element.columns);
    }
}

void AccessRights::revoke(const AccessRightsElements & elements, std::string_view current_database)
{
    for (const auto & element : elements)
        revoke(element, current_database);
}


AccessRightsElements AccessRights::getGrants() const
{
    AccessRightsElements grants;
    getGrantsAndPartialRevokesImpl(&grants, nullptr);
    return grants;
}

AccessRightsElements AccessRights::getPartialRevokes() const
{
    AccessRightsElements partial_revokes;
    getGrantsAndPartialRevokesImpl(nullptr, &partial_revokes);
    return partial_revokes;
}

AccessRights::GrantsAndPartialRevokes AccessRights::getGrantsAndPartialRevokes() const
{
    GrantsAndPartialRevokes res;
    getGrantsAndPartialRevokesImpl(&res.grants, &res.revokes);
    return res;
}


void AccessRights::getGrantsAndPartialRevokesImpl(AccessRightsElements * out_grants, AccessRightsElements * out_partial_revokes) const
{
    if (!root)
        return;
    auto global_access = root->access;
    if (out_grants && global_access)
        out_grants->push_back({global_access});
    if (root->children)
    {
        for (const auto & [db_name, db_node] : *root->children)
        {
            if (out_grants)
            {
                if (auto db_grants = db_node.access - global_access)
                    out_grants->push_back({db_grants, db_name});
            }
            if (out_partial_revokes)
            {
                if (auto db_partial_revokes = global_access - db_node.access)
                    out_partial_revokes->push_back({db_partial_revokes, db_name});
            }
            if (db_node.children)
            {
                for (const auto & [table_name, table_node] : *db_node.children)
                {
                    if (out_grants)
                    {
                        if (auto table_grants = table_node.access - db_node.access)
                            out_grants->push_back({table_grants, db_name, table_name});
                    }
                    if (out_partial_revokes)
                    {
                        if (auto table_partial_revokes = db_node.access - table_node.access)
                            out_partial_revokes->push_back({table_partial_revokes, db_name, table_name});
                    }
                    if (table_node.children)
                    {
                        for (const auto & [column_name, column_node] : *table_node.children)
                        {
                            if (out_grants)
                            {
                                if (auto column_grants = column_node.access - table_node.access)
                                    out_grants->push_back({column_grants, db_name, table_name, column_name});
                            }
                            if (out_partial_revokes)
                            {
                                if (auto column_partial_revokes = table_node.access - column_node.access)
                                    out_partial_revokes->push_back({column_partial_revokes, db_name, table_name, column_name});
                            }
                        }

                    }
                }
            }
        }
    }
}


String AccessRights::toString() const
{
    String res;
    auto gr = getGrantsAndPartialRevokes();
    if (!gr.grants.empty())
    {
        res += "GRANT ";
        res += gr.grants.toString();
    }
    if (!gr.revokes.empty())
    {
        if (!res.empty())
            res += ", ";
        res += "REVOKE ";
        res += gr.revokes.toString();
    }
    if (res.empty())
        res = "GRANT USAGE ON *.*";
    return res;
}


template <typename... Args>
bool AccessRights::isGrantedImpl(const AccessFlags & flags, const Args &... args) const
{
    if (!root)
        return flags.isEmpty();
    return root->isGranted(flags, args...);
}

bool AccessRights::isGranted(const AccessFlags & flags) const { return isGrantedImpl(flags); }
bool AccessRights::isGranted(const AccessFlags & flags, const std::string_view & database) const { return isGrantedImpl(flags, database); }
bool AccessRights::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { return isGrantedImpl(flags, database, table); }
bool AccessRights::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return isGrantedImpl(flags, database, table, column); }
bool AccessRights::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return isGrantedImpl(flags, database, table, columns); }
bool AccessRights::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return isGrantedImpl(flags, database, table, columns); }

bool AccessRights::isGranted(const AccessRightsElement & element, std::string_view current_database) const
{
    if (element.any_database)
    {
        return isGranted(element.access_flags);
    }
    else if (element.any_table)
    {
        if (element.database.empty())
            return isGranted(element.access_flags, checkCurrentDatabase(current_database));
        else
            return isGranted(element.access_flags, element.database);
    }
    else if (element.any_column)
    {
        if (element.database.empty())
            return isGranted(element.access_flags, checkCurrentDatabase(current_database), element.table);
        else
            return isGranted(element.access_flags, element.database, element.table);
    }
    else
    {
        if (element.database.empty())
            return isGranted(element.access_flags, checkCurrentDatabase(current_database), element.table, element.columns);
        else
            return isGranted(element.access_flags, element.database, element.table, element.columns);
    }
}

bool AccessRights::isGranted(const AccessRightsElements & elements, std::string_view current_database) const
{
    for (const auto & element : elements)
        if (!isGranted(element, current_database))
            return false;
    return true;
}


bool operator ==(const AccessRights & left, const AccessRights & right)
{
    if (!left.root)
        return !right.root;
    if (!right.root)
        return false;
    return *left.root == *right.root;
}


void AccessRights::merge(const AccessRights & other)
{
    if (!root)
    {
        *this = other;
        return;
    }
    if (other.root)
    {
        root->merge(*other.root, Helper::instance());
        if (!root->access && !root->children)
            root = nullptr;
    }
}


void AccessRights::logTree() const
{
    auto * log = &Poco::Logger::get("AccessRights");
    if (root)
        root->logTree(log);
    else
        LOG_TRACE_FORMATTED(log, "Tree: NULL");
}
}
