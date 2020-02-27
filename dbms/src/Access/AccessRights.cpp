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

    enum RevokeMode
    {
        NORMAL_REVOKE_MODE,  /// for AccessRights::revoke()
        PARTIAL_REVOKE_MODE, /// for AccessRights::partialRevoke()
        FULL_REVOKE_MODE,    /// for AccessRights::fullRevoke()
    };

    struct Helper
    {
        static const Helper & instance()
        {
            static const Helper res;
            return res;
        }

        const AccessFlags database_level_flags = AccessFlags::databaseLevel();
        const AccessFlags table_level_flags = AccessFlags::tableLevel();
        const AccessFlags column_level_flags = AccessFlags::columnLevel();
        const AccessFlags show_flag = AccessType::SHOW;
        const AccessFlags exists_flag = AccessType::EXISTS;
        const AccessFlags create_table_flag = AccessType::CREATE_TABLE;
        const AccessFlags create_temporary_table_flag = AccessType::CREATE_TEMPORARY_TABLE;
    };
}


struct AccessRights::Node
{
public:
    std::shared_ptr<const String> node_name;
    Level level = GLOBAL_LEVEL;
    AccessFlags explicit_grants;
    AccessFlags partial_revokes;
    AccessFlags inherited_access; /// the access inherited from the parent node
    AccessFlags raw_access;       /// raw_access = (inherited_access - partial_revokes) | explicit_grants
    AccessFlags access;           /// access = raw_access | implicit_access
    AccessFlags min_access;       /// min_access = access & child[0].access & ... | child[N-1].access
    AccessFlags max_access;       /// max_access = access | child[0].access | ... | child[N-1].access
    std::unique_ptr<std::unordered_map<std::string_view, Node>> children;

    Node() = default;
    Node(const Node & src) { *this = src; }

    Node & operator =(const Node & src)
    {
        node_name = src.node_name;
        level = src.level;
        inherited_access = src.inherited_access;
        explicit_grants = src.explicit_grants;
        partial_revokes = src.partial_revokes;
        raw_access = src.raw_access;
        access = src.access;
        min_access = src.min_access;
        max_access = src.max_access;
        if (src.children)
            children = std::make_unique<std::unordered_map<std::string_view, Node>>(*src.children);
        else
            children = nullptr;
        return *this;
    }

    void grant(AccessFlags access_to_grant, const Helper & helper)
    {
        if (!access_to_grant)
            return;

        if (level == GLOBAL_LEVEL)
        {
            /// Everything can be granted on the global level.
        }
        else if (level == DATABASE_LEVEL)
        {
            AccessFlags grantable = access_to_grant & helper.database_level_flags;
            if (!grantable)
                throw Exception(access_to_grant.toString() + " cannot be granted on the database level", ErrorCodes::INVALID_GRANT);
            access_to_grant = grantable;
        }
        else if (level == TABLE_LEVEL)
        {
            AccessFlags grantable = access_to_grant & helper.table_level_flags;
            if (!grantable)
                throw Exception(access_to_grant.toString() + " cannot be granted on the table level", ErrorCodes::INVALID_GRANT);
            access_to_grant = grantable;
        }
        else if (level == COLUMN_LEVEL)
        {
            AccessFlags grantable = access_to_grant & helper.column_level_flags;
            if (!grantable)
                throw Exception(access_to_grant.toString() + " cannot be granted on the column level", ErrorCodes::INVALID_GRANT);
            access_to_grant = grantable;
        }

        AccessFlags new_explicit_grants = access_to_grant - partial_revokes;
        if (level == TABLE_LEVEL)
            removeExplicitGrantsRec(new_explicit_grants);
        removePartialRevokesRec(access_to_grant);
        explicit_grants |= new_explicit_grants;

        calculateAllAccessRec(helper);
    }

    template <typename ... Args>
    void grant(const AccessFlags & access_to_grant, const Helper & helper, const std::string_view & name, const Args &... subnames)
    {
        auto & child = getChild(name);
        child.grant(access_to_grant, helper, subnames...);
        eraseChildIfEmpty(child);
        calculateImplicitAccess(helper);
        calculateMinAndMaxAccess();
    }

    template <typename StringT>
    void grant(const AccessFlags & access_to_grant, const Helper & helper, const std::vector<StringT> & names)
    {
        for (const auto & name : names)
        {
            auto & child = getChild(name);
            child.grant(access_to_grant, helper);
            eraseChildIfEmpty(child);
        }
        calculateImplicitAccess(helper);
        calculateMinAndMaxAccess();
    }

    template <int mode>
    void revoke(const AccessFlags & access_to_revoke, const Helper & helper)
    {
        if constexpr (mode == NORMAL_REVOKE_MODE)
        {
            if (level == TABLE_LEVEL)
                removeExplicitGrantsRec(access_to_revoke);
            else
                removeExplicitGrants(access_to_revoke);
        }
        else if constexpr (mode == PARTIAL_REVOKE_MODE)
        {
            AccessFlags new_partial_revokes = access_to_revoke - explicit_grants;
            if (level == TABLE_LEVEL)
                removeExplicitGrantsRec(access_to_revoke);
            else
                removeExplicitGrants(access_to_revoke);
            removePartialRevokesRec(new_partial_revokes);
            partial_revokes |= new_partial_revokes;
        }
        else /// mode == FULL_REVOKE_MODE
        {
            AccessFlags new_partial_revokes = access_to_revoke - explicit_grants;
            removeExplicitGrantsRec(access_to_revoke);
            removePartialRevokesRec(new_partial_revokes);
            partial_revokes |= new_partial_revokes;
        }
        calculateAllAccessRec(helper);
    }

    template <int mode, typename... Args>
    void revoke(const AccessFlags & access_to_revoke, const Helper & helper, const std::string_view & name, const Args &... subnames)
    {
        Node * child;
        if (mode == NORMAL_REVOKE_MODE)
        {
            if (!(child = tryGetChild(name)))
                return;
        }
        else
            child = &getChild(name);

        child->revoke<mode>(access_to_revoke, helper, subnames...);
        eraseChildIfEmpty(*child);
        calculateImplicitAccess(helper);
        calculateMinAndMaxAccess();
    }

    template <int mode, typename StringT>
    void revoke(const AccessFlags & access_to_revoke, const Helper & helper, const std::vector<StringT> & names)
    {
        Node * child;
        for (const auto & name : names)
        {
            if (mode == NORMAL_REVOKE_MODE)
            {
                if (!(child = tryGetChild(name)))
                    continue;
            }
            else
                child = &getChild(name);

            child->revoke<mode>(access_to_revoke, helper);
            eraseChildIfEmpty(*child);
        }
        calculateImplicitAccess(helper);
        calculateMinAndMaxAccess();
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
            return access.contains(flags);
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
                if (!access.contains(flags))
                    return false;
            }
        }
        return true;
    }

    friend bool operator ==(const Node & left, const Node & right)
    {
        if ((left.explicit_grants != right.explicit_grants) || (left.partial_revokes != right.partial_revokes))
            return false;

        if (!left.children)
            return !right.children;

        if (!right.children)
            return false;
        return *left.children == *right.children;
    }

    friend bool operator!=(const Node & left, const Node & right) { return !(left == right); }

    bool isEmpty() const
    {
        return !explicit_grants && !partial_revokes && !children;
    }

    void merge(const Node & other, const Helper & helper)
    {
        mergeRawAccessRec(other);
        calculateGrantsAndPartialRevokesRec();
        calculateAllAccessRec(helper);
    }

    void traceTree(Poco::Logger * log) const
    {
        LOG_TRACE(log, "Tree(" << level << "): name=" << (node_name ? *node_name : "NULL")
                  << ", explicit_grants=" << explicit_grants.toString()
                  << ", partial_revokes=" << partial_revokes.toString()
                  << ", inherited_access=" << inherited_access.toString()
                  << ", raw_access=" << raw_access.toString()
                  << ", access=" << access.toString()
                  << ", min_access=" << min_access.toString()
                  << ", max_access=" << max_access.toString()
                  << ", num_children=" << (children ? children->size() : 0));
        if (children)
        {
            for (auto & child : *children | boost::adaptors::map_values)
                child.traceTree(log);
        }
    }

private:
    Node * tryGetChild(const std::string_view & name)
    {
        if (!children)
            return nullptr;
        auto it = children->find(name);
        if (it == children->end())
            return nullptr;
        return &it->second;
    }

    const Node * tryGetChild(const std::string_view & name) const
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
        new_child.inherited_access = raw_access;
        new_child.raw_access = raw_access;
        return new_child;
    }

    void eraseChildIfEmpty(Node & child)
    {
        if (!child.isEmpty())
            return;
        auto it = children->find(*child.node_name);
        children->erase(it);
        if (children->empty())
            children = nullptr;
    }

    void calculateImplicitAccess(const Helper & helper)
    {
        access = raw_access;
        if (access & helper.database_level_flags)
            access |= helper.show_flag | helper.exists_flag;
        else if ((level >= DATABASE_LEVEL) && children)
            access |= helper.exists_flag;

        if ((level == GLOBAL_LEVEL) && (access & helper.create_table_flag))
            access |= helper.create_temporary_table_flag;
    }

    void calculateMinAndMaxAccess()
    {
        min_access = access;
        max_access = access;
        if (children)
        {
            for (const auto & child : *children | boost::adaptors::map_values)
            {
                min_access &= child.min_access;
                max_access |= child.max_access;
            }
        }
    }

    void calculateAllAccessRec(const Helper & helper)
    {
        partial_revokes &= inherited_access;
        raw_access = (inherited_access - partial_revokes) | explicit_grants;

        /// Traverse tree.
        if (children)
        {
            for (auto it = children->begin(); it != children->end();)
            {
                auto & child = it->second;
                child.inherited_access = raw_access;
                child.calculateAllAccessRec(helper);
                if (child.isEmpty())
                    it = children->erase(it);
                else
                    ++it;
            }
            if (children->empty())
                children = nullptr;
        }

        calculateImplicitAccess(helper);
        calculateMinAndMaxAccess();
    }

    void removeExplicitGrants(const AccessFlags & change)
    {
        explicit_grants -= change;
    }

    void removeExplicitGrantsRec(const AccessFlags & change)
    {
        removeExplicitGrants(change);
        if (children)
        {
            for (auto & child : *children | boost::adaptors::map_values)
                child.removeExplicitGrantsRec(change);
        }
    }

    void removePartialRevokesRec(const AccessFlags & change)
    {
        partial_revokes -= change;
        if (children)
        {
            for (auto & child : *children | boost::adaptors::map_values)
                child.removePartialRevokesRec(change);
        }
    }

    void mergeRawAccessRec(const Node & rhs)
    {
        if (rhs.children)
        {
            for (const auto & [rhs_childname, rhs_child] : *rhs.children)
                getChild(rhs_childname).mergeRawAccessRec(rhs_child);
        }
        raw_access |= rhs.raw_access;
        if (children)
        {
            for (auto & [lhs_childname, lhs_child] : *children)
            {
                lhs_child.inherited_access = raw_access;
                if (!rhs.tryGetChild(lhs_childname))
                    lhs_child.raw_access |= rhs.raw_access;
            }
        }
    }

    void calculateGrantsAndPartialRevokesRec()
    {
        explicit_grants = raw_access - inherited_access;
        partial_revokes = inherited_access - raw_access;
        if (children)
        {
            for (auto & child : *children | boost::adaptors::map_values)
                child.calculateGrantsAndPartialRevokesRec();
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
void AccessRights::grantImpl(const AccessFlags & access, const Args &... args)
{
    if (!root)
        root = std::make_unique<Node>();
    root->grant(access, Helper::instance(), args...);
    if (root->isEmpty())
        root = nullptr;
}

void AccessRights::grantImpl(const AccessRightsElement & element, std::string_view current_database)
{
    if (element.any_database)
    {
        grantImpl(element.access_flags);
    }
    else if (element.any_table)
    {
        if (element.database.empty())
            grantImpl(element.access_flags, current_database);
        else
            grantImpl(element.access_flags, element.database);
    }
    else if (element.any_column)
    {
        if (element.database.empty())
            grantImpl(element.access_flags, current_database, element.table);
        else
            grantImpl(element.access_flags, element.database, element.table);
    }
    else
    {
        if (element.database.empty())
            grantImpl(element.access_flags, current_database, element.table, element.columns);
        else
            grantImpl(element.access_flags, element.database, element.table, element.columns);
    }
}

void AccessRights::grantImpl(const AccessRightsElements & elements, std::string_view current_database)
{
    for (const auto & element : elements)
        grantImpl(element, current_database);
}

void AccessRights::grant(const AccessFlags & access) { grantImpl(access); }
void AccessRights::grant(const AccessFlags & access, const std::string_view & database) { grantImpl(access, database); }
void AccessRights::grant(const AccessFlags & access, const std::string_view & database, const std::string_view & table) { grantImpl(access, database, table); }
void AccessRights::grant(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) { grantImpl(access, database, table, column); }
void AccessRights::grant(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { grantImpl(access, database, table, columns); }
void AccessRights::grant(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) { grantImpl(access, database, table, columns); }
void AccessRights::grant(const AccessRightsElement & element, std::string_view current_database) { grantImpl(element, current_database); }
void AccessRights::grant(const AccessRightsElements & elements, std::string_view current_database) { grantImpl(elements, current_database); }

template <int mode, typename... Args>
void AccessRights::revokeImpl(const AccessFlags & access, const Args &... args)
{
    if (!root)
        return;
    root->revoke<mode>(access, Helper::instance(), args...);
    if (root->isEmpty())
        root = nullptr;
}

template <int mode>
void AccessRights::revokeImpl(const AccessRightsElement & element, std::string_view current_database)
{
    if (element.any_database)
    {
        revokeImpl<mode>(element.access_flags);
    }
    else if (element.any_table)
    {
        if (element.database.empty())
            revokeImpl<mode>(element.access_flags, current_database);
        else
            revokeImpl<mode>(element.access_flags, element.database);
    }
    else if (element.any_column)
    {
        if (element.database.empty())
            revokeImpl<mode>(element.access_flags, current_database, element.table);
        else
            revokeImpl<mode>(element.access_flags, element.database, element.table);
    }
    else
    {
        if (element.database.empty())
            revokeImpl<mode>(element.access_flags, current_database, element.table, element.columns);
        else
            revokeImpl<mode>(element.access_flags, element.database, element.table, element.columns);
    }
}

template <int mode>
void AccessRights::revokeImpl(const AccessRightsElements & elements, std::string_view current_database)
{
    for (const auto & element : elements)
        revokeImpl<mode>(element, current_database);
}

void AccessRights::revoke(const AccessFlags & access) { revokeImpl<NORMAL_REVOKE_MODE>(access); }
void AccessRights::revoke(const AccessFlags & access, const std::string_view & database) { revokeImpl<NORMAL_REVOKE_MODE>(access, database); }
void AccessRights::revoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table) { revokeImpl<NORMAL_REVOKE_MODE>(access, database, table); }
void AccessRights::revoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) { revokeImpl<NORMAL_REVOKE_MODE>(access, database, table, column); }
void AccessRights::revoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { revokeImpl<NORMAL_REVOKE_MODE>(access, database, table, columns); }
void AccessRights::revoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) { revokeImpl<NORMAL_REVOKE_MODE>(access, database, table, columns); }
void AccessRights::revoke(const AccessRightsElement & element, std::string_view current_database) { revokeImpl<NORMAL_REVOKE_MODE>(element, current_database); }
void AccessRights::revoke(const AccessRightsElements & elements, std::string_view current_database) { revokeImpl<NORMAL_REVOKE_MODE>(elements, current_database); }

void AccessRights::partialRevoke(const AccessFlags & access) { revokeImpl<PARTIAL_REVOKE_MODE>(access); }
void AccessRights::partialRevoke(const AccessFlags & access, const std::string_view & database) { revokeImpl<PARTIAL_REVOKE_MODE>(access, database); }
void AccessRights::partialRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table) { revokeImpl<PARTIAL_REVOKE_MODE>(access, database, table); }
void AccessRights::partialRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) { revokeImpl<PARTIAL_REVOKE_MODE>(access, database, table, column); }
void AccessRights::partialRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { revokeImpl<PARTIAL_REVOKE_MODE>(access, database, table, columns); }
void AccessRights::partialRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) { revokeImpl<PARTIAL_REVOKE_MODE>(access, database, table, columns); }
void AccessRights::partialRevoke(const AccessRightsElement & element, std::string_view current_database) { revokeImpl<PARTIAL_REVOKE_MODE>(element, current_database); }
void AccessRights::partialRevoke(const AccessRightsElements & elements, std::string_view current_database) { revokeImpl<PARTIAL_REVOKE_MODE>(elements, current_database); }

void AccessRights::fullRevoke(const AccessFlags & access) { revokeImpl<FULL_REVOKE_MODE>(access); }
void AccessRights::fullRevoke(const AccessFlags & access, const std::string_view & database) { revokeImpl<FULL_REVOKE_MODE>(access, database); }
void AccessRights::fullRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table) { revokeImpl<FULL_REVOKE_MODE>(access, database, table); }
void AccessRights::fullRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) { revokeImpl<FULL_REVOKE_MODE>(access, database, table, column); }
void AccessRights::fullRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { revokeImpl<FULL_REVOKE_MODE>(access, database, table, columns); }
void AccessRights::fullRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) { revokeImpl<FULL_REVOKE_MODE>(access, database, table, columns); }
void AccessRights::fullRevoke(const AccessRightsElement & element, std::string_view current_database) { revokeImpl<FULL_REVOKE_MODE>(element, current_database); }
void AccessRights::fullRevoke(const AccessRightsElements & elements, std::string_view current_database) { revokeImpl<FULL_REVOKE_MODE>(elements, current_database); }


AccessRights::Elements AccessRights::getElements() const
{
    if (!root)
        return {};
    Elements res;
    if (root->explicit_grants)
        res.grants.push_back({root->explicit_grants});
    if (root->children)
    {
        for (const auto & [db_name, db_node] : *root->children)
        {
            if (db_node.partial_revokes)
                res.partial_revokes.push_back({db_node.partial_revokes, db_name});
            if (db_node.explicit_grants)
                res.grants.push_back({db_node.explicit_grants, db_name});
            if (db_node.children)
            {
                for (const auto & [table_name, table_node] : *db_node.children)
                {
                    if (table_node.partial_revokes)
                        res.partial_revokes.push_back({table_node.partial_revokes, db_name, table_name});
                    if (table_node.explicit_grants)
                        res.grants.push_back({table_node.explicit_grants, db_name, table_name});
                    if (table_node.children)
                    {
                        for (const auto & [column_name, column_node] : *table_node.children)
                        {
                            if (column_node.partial_revokes)
                                res.partial_revokes.push_back({column_node.partial_revokes, db_name, table_name, column_name});
                            if (column_node.explicit_grants)
                                res.grants.push_back({column_node.explicit_grants, db_name, table_name, column_name});
                        }
                    }
                }
            }
        }
    }
    return res;
}


String AccessRights::toString() const
{
    auto elements = getElements();
    String res;
    if (!elements.grants.empty())
    {
        res += "GRANT ";
        res += elements.grants.toString();
    }
    if (!elements.partial_revokes.empty())
    {
        if (!res.empty())
            res += ", ";
        res += "REVOKE ";
        res += elements.partial_revokes.toString();
    }
    if (res.empty())
        res = "GRANT USAGE ON *.*";
    return res;
}


template <typename... Args>
bool AccessRights::isGrantedImpl(const AccessFlags & access, const Args &... args) const
{
    if (!root)
        return access.isEmpty();
    return root->isGranted(access, args...);
}

bool AccessRights::isGrantedImpl(const AccessRightsElement & element, std::string_view current_database) const
{
    if (element.any_database)
    {
        return isGrantedImpl(element.access_flags);
    }
    else if (element.any_table)
    {
        if (element.database.empty())
            return isGrantedImpl(element.access_flags, current_database);
        else
            return isGrantedImpl(element.access_flags, element.database);
    }
    else if (element.any_column)
    {
        if (element.database.empty())
            return isGrantedImpl(element.access_flags, current_database, element.table);
        else
            return isGrantedImpl(element.access_flags, element.database, element.table);
    }
    else
    {
        if (element.database.empty())
            return isGrantedImpl(element.access_flags, current_database, element.table, element.columns);
        else
            return isGrantedImpl(element.access_flags, element.database, element.table, element.columns);
    }
}

bool AccessRights::isGrantedImpl(const AccessRightsElements & elements, std::string_view current_database) const
{
    for (const auto & element : elements)
        if (!isGrantedImpl(element, current_database))
            return false;
    return true;
}

bool AccessRights::isGranted(const AccessFlags & access) const { return isGrantedImpl(access); }
bool AccessRights::isGranted(const AccessFlags & access, const std::string_view & database) const { return isGrantedImpl(access, database); }
bool AccessRights::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table) const { return isGrantedImpl(access, database, table); }
bool AccessRights::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return isGrantedImpl(access, database, table, column); }
bool AccessRights::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return isGrantedImpl(access, database, table, columns); }
bool AccessRights::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return isGrantedImpl(access, database, table, columns); }
bool AccessRights::isGranted(const AccessRightsElement & element, std::string_view current_database) const { return isGrantedImpl(element, current_database); }
bool AccessRights::isGranted(const AccessRightsElements & elements, std::string_view current_database) const { return isGrantedImpl(elements, current_database); }


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
        if (root->isEmpty())
            root = nullptr;
    }
}


void AccessRights::traceTree() const
{
    auto * log = &Poco::Logger::get("AccessRights");
    if (root)
        root->traceTree(log);
    else
        LOG_TRACE(log, "Tree: NULL");
}
}
