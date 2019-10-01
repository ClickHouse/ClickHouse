#include <ACL/AllowedDatabases.h>
#include <Common/Exception.h>
#include <Parsers/ASTGrantQuery.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_ENOUGH_PRIVILEGES;
}


const AllowedDatabases::AccessType AllowedDatabases::USAGE = ASTGrantQuery::USAGE;
const AllowedDatabases::AccessType AllowedDatabases::SELECT = ASTGrantQuery::SELECT;
const AllowedDatabases::AccessType AllowedDatabases::INSERT = ASTGrantQuery::INSERT;
const AllowedDatabases::AccessType AllowedDatabases::DELETE = ASTGrantQuery::DELETE;
const AllowedDatabases::AccessType AllowedDatabases::ALTER = ASTGrantQuery::ALTER;
const AllowedDatabases::AccessType AllowedDatabases::CREATE = ASTGrantQuery::CREATE;
const AllowedDatabases::AccessType AllowedDatabases::DROP = ASTGrantQuery::DROP;

String AllowedDatabases::accessTypeToString(AccessType access)
{
    return ASTGrantQuery::accessTypeToString(access);
}

String AllowedDatabases::accessToString(AccessType access)
{
    return ASTGrantQuery::accessToString(access);
}

String AllowedDatabases::accessToString(AccessType access, const String & database)
{
    return ASTGrantQuery::accessToString(access, database);
}

String AllowedDatabases::accessToString(AccessType access, const String & database, const String & table)
{
    return ASTGrantQuery::accessToString(access, database, table);
}

String AllowedDatabases::accessToString(AccessType access, const String & database, const String & table, const String & column)
{
    return ASTGrantQuery::accessToString(access, database, table, column);
}

String AllowedDatabases::accessToString(AccessType access, const String & database, const String & table, const Strings & columns)
{
    return ASTGrantQuery::accessToString(access, database, table, columns);
}


AllowedDatabases::Node::Node(Node && src) { *this = src; }
AllowedDatabases::Node & AllowedDatabases::Node::operator =(Node && src)
{
    access = src.access;
    grants = src.grants;
    children = std::move(src.children);
    if (children)
    {
        for (auto & [child_name, child_node] : *children)
            child_node.parent = this;
    }
    return *this;
}


AllowedDatabases::Node::Node(const Node & src) { *this = src; }
AllowedDatabases::Node & AllowedDatabases::Node::operator =(const Node & src)
{
    access = src.access;
    grants = src.grants;
    if (src.children)
    {
        children = std::make_unique<ChildrenMap>(*src.children);
        for (auto & [child_name, child_node] : *children)
            child_node.parent = this;
    }
    else
    {
        children.reset();
    }
    return *this;
}


AllowedDatabases::Node * AllowedDatabases::Node::find(const String & child_name)
{
    if (!children)
        return nullptr;
    auto it = children->find(child_name);
    if (it != children->end())
        return &it->second;
    return nullptr;
}


const AllowedDatabases::Node * AllowedDatabases::Node::find(const String & child_name) const
{
    if (!children)
        return nullptr;
    auto it = children->find(child_name);
    if (it != children->end())
        return &it->second;
    return nullptr;
}


template <typename ChildrenMapT>
typename ChildrenMapT::iterator AllowedDatabases::Node::getIterator(const String & child_name)
{
    if (children)
    {
        auto it = children->find(child_name);
        if (it != children->end())
            return it;
    }
    else
        children = std::make_unique<ChildrenMap>();

    auto [it, inserted] = children->try_emplace(child_name);
    if (inserted)
    {
        it->second.access = access;
        it->second.parent = this;
    }
    return it;
}


AllowedDatabases::Node & AllowedDatabases::Node::get(const String & child_name)
{
    return getIterator(child_name)->second;
}


AllowedDatabases::AccessType AllowedDatabases::Node::getAccess(const String & name) const
{
    const Node * child = find(name);
    return child ? child->access : access;
}


AllowedDatabases::AccessType AllowedDatabases::Node::getAccess(const Strings & names) const
{
    if (names.empty())
        return 0;
    AccessType result = getAccess(names[0]);
    for (size_t i = 1; i != names.size(); ++i)
        result &= getAccess(names[i]);
    return result;
}


AllowedDatabases::AccessType AllowedDatabases::Node::getAccess(const String & name1, const String & name2) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(name2) : access;
}


AllowedDatabases::AccessType AllowedDatabases::Node::getAccess(const String & name1, const Strings & names2) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(names2) : access;
}


AllowedDatabases::AccessType AllowedDatabases::Node::getAccess(const String & name1, const String & name2, const String & name3) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(name2, name3) : access;
}


AllowedDatabases::AccessType AllowedDatabases::Node::getAccess(const String & name1, const String & name2, const Strings & names3) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(name2, names3) : access;
}


bool AllowedDatabases::Node::grant(AccessType add_access)
{
    add_access &= ~grants; /// Exclude access types which are already granted.
    if (!add_access)
        return false; /// Nothing to grant.

    /// Cancelling of a partial revoke isn't considered as grant.
    grants |= add_access & ~getPartialRevokes();

    /// Change access types for the children.
    addAccess(add_access);
    return true;
}


void AllowedDatabases::Node::addAccess(AccessType add_access)
{
    access |= add_access;
    if (children)
    {
        for (auto it = children->begin(); it != children->end();)
        {
            auto & child = it->second;
            child.addAccess(add_access);
            eraseOrIncrement(it);
        }
        if (children->empty())
            children.reset();
    }
}


bool AllowedDatabases::Node::grant(AccessType add_access, const String & name)
{
    auto it = getIterator(name);
    if (!it->second.grant(add_access))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::grant(AccessType add_access, const Strings & names)
{
    bool changed = false;
    for (const String & name : names)
        changed |= grant(add_access, name);
    return changed;
}


bool AllowedDatabases::Node::grant(AccessType add_access, const String & name1, const String & name2)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, name2))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::grant(AccessType add_access, const String & name1, const Strings & names2)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, names2))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::grant(AccessType add_access, const String & name1, const String & name2, const String & name3)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, name2, name3))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::grant(AccessType add_access, const String & name1, const String & name2, const Strings & names3)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, name2, names3))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::revoke(AccessType remove_access, bool partial_revokes)
{
    if (partial_revokes)
        remove_access &= access; /// Skip access types we don't have.
    else
        remove_access &= grants; /// Skip access types which are not granted.

    if (!remove_access)
        return false; /// Nothing to revoke.

    /// If (remove_access & ~grants) != 0 then it's a partial revoke.
    /// Partial revokes are implemented like https://dev.mysql.com/doc/refman/8.0/en/partial-revokes.html
    AccessType new_partial_revokes = remove_access & ~grants;

    grants &= ~remove_access;

    /// Change access types for the children.
    /// We don't change access if it's granted at parent level too.
    removeAccess((remove_access & ~getParentAccess()) | new_partial_revokes);
    return true;
}


void AllowedDatabases::Node::removeAccess(AccessType remove_access)
{
    remove_access &= ~grants;
    if (!remove_access)
        return;
    access &= ~remove_access;
    if (children)
    {
        for (auto it = children->begin(); it != children->end();)
        {
            auto & child = it->second;
            child.removeAccess(remove_access);
            eraseOrIncrement(it);
        }
        if (children->empty())
            children.reset();
    }
}


bool AllowedDatabases::Node::revoke(AccessType add_access, const String & name, bool partial_revokes)
{
    auto it = getIterator(name);
    if (!it->second.revoke(add_access, partial_revokes))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::revoke(AccessType add_access, const Strings & names, bool partial_revokes)
{
    bool changed = false;
    for (const String & name : names)
        changed |= revoke(add_access, name, partial_revokes);
    return changed;
}


bool AllowedDatabases::Node::revoke(AccessType add_access, const String & name1, const String & name2, bool partial_revokes)
{
    auto it = getIterator(name1);
    if (!it->second.revoke(add_access, name2, partial_revokes))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::revoke(AccessType add_access, const String & name1, const Strings & names2, bool partial_revokes)
{
    auto it = getIterator(name1);
    if (!it->second.revoke(add_access, names2, partial_revokes))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::revoke(AccessType add_access, const String & name1, const String & name2, const String & name3, bool partial_revokes)
{
    auto it = getIterator(name1);
    if (!it->second.revoke(add_access, name2, name3, partial_revokes))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::revoke(AccessType add_access, const String & name1, const String & name2, const Strings & names3, bool partial_revokes)
{
    auto it = getIterator(name1);
    if (!it->second.revoke(add_access, name2, names3, partial_revokes))
        return false;
    eraseOrIncrement(it);
    return true;
}


template <typename ChildrenMapT>
void AllowedDatabases::Node::eraseOrIncrement(typename ChildrenMapT::iterator & it)
{
    auto & child = it->second;
    if (!child.children && !child.grants && (access == child.access))
        it = children->erase(it);
    else
        ++it;
}


void AllowedDatabases::Node::merge(const Node & other)
{
    if (other.children)
    {
        for (const auto & [name, other_child] : *other.children)
            get(name); /// Ensure the node is created.
    }

    access |= other.access;
    grants = access & ~getParentAccess();

    if (children)
    {
        for (auto it = children->begin(); it != children->end();)
        {
            auto & child = it->second;
            const auto * other_child = other.find(it->first);
            if (other_child)
                child.merge(*other_child);
            else
                child.addAccessRecalcGrants(other.access);
            eraseOrIncrement(it);
        }
    }
}


void AllowedDatabases::Node::addAccessRecalcGrants(AccessType add_access)
{
    access |= add_access;
    grants = access & ~getParentAccess();
    if (children)
    {
        for (auto it = children->begin(); it != children->end();)
        {
            auto & child = it->second;
            child.addAccessRecalcGrants(add_access);
            eraseOrIncrement(it);
        }
        if (children->empty())
            children.reset();
    }
}


bool AllowedDatabases::Node::operator ==(const AllowedDatabases::Node & other) const
{
    if ((access != other.access) || (grants != other.grants))
        return false;
    if (!!children != !!other.children)
        return false;
    if (children && (*children != *other.children))
        return false;
    return true;
}


AllowedDatabases::AllowedDatabases() = default;
AllowedDatabases::~AllowedDatabases() = default;
AllowedDatabases::AllowedDatabases(const AllowedDatabases & src) = default;
AllowedDatabases & AllowedDatabases::operator =(const AllowedDatabases & src) = default;
AllowedDatabases::AllowedDatabases(AllowedDatabases && src) = default;
AllowedDatabases & AllowedDatabases::operator =(AllowedDatabases && src) = default;


bool AllowedDatabases::isEmpty() const
{
    return root.isEmpty();
}


void AllowedDatabases::clear()
{
    root = Node{};
}


bool AllowedDatabases::grant(AccessType access)
{
    access &= ASTGrantQuery::ALL_DATABASE_LEVEL;
    return root.grant(access);
}


bool AllowedDatabases::grant(AccessType access, const String & database)
{
    access &= ASTGrantQuery::ALL_DATABASE_LEVEL;
    return root.grant(access, database);
}


bool AllowedDatabases::grant(AccessType access, const String & database, const String & table)
{
    access &= ASTGrantQuery::ALL_TABLE_LEVEL;
    return root.grant(access, database, table);
}


bool AllowedDatabases::grant(AccessType access, const String & database, const String & table, const String & column)
{
    access &= ASTGrantQuery::ALL_COLUMN_LEVEL;
    return root.grant(access, database, table, column);
}


bool AllowedDatabases::grant(AccessType access, const String & database, const String & table, const Strings & columns)
{
    access &= ASTGrantQuery::ALL_COLUMN_LEVEL;
    return root.grant(access, database, table, columns);
}


bool AllowedDatabases::revoke(AccessType access)
{
    return root.revoke(access);
}


bool AllowedDatabases::revoke(AccessType access, const String & database, bool partial_revokes)
{
    return root.revoke(access, database, partial_revokes);
}


bool AllowedDatabases::revoke(AccessType access, const String & database, const String & table, bool partial_revokes)
{
    return root.revoke(access, database, table, partial_revokes);
}


bool AllowedDatabases::revoke(AccessType access, const String & database, const String & table, const String & column, bool partial_revokes)
{
    return root.revoke(access, database, table, column, partial_revokes);
}


bool AllowedDatabases::revoke(AccessType access, const String & database, const String & table, const Strings & columns, bool partial_revokes)
{
    return root.revoke(access, database, table, columns, partial_revokes);
}


void AllowedDatabases::checkAccess(AccessType access) const
{
    checkAccess(String(), access);
}


void AllowedDatabases::checkAccess(AccessType access, const String & database) const
{
    checkAccess(String(), access, database);
}


void AllowedDatabases::checkAccess(AccessType access, const String & database, const String & table) const
{
    checkAccess(String(), access, database, table);
}


void AllowedDatabases::checkAccess(AccessType access, const String & database, const String & table, const String & column) const
{
    checkAccess(String(), access, database, table, column);
}


void AllowedDatabases::checkAccess(AccessType access, const String & database, const String & table, const Strings & columns) const
{
    checkAccess(String(), access, database, table, columns);
}


void AllowedDatabases::checkAccess(const String & user_name, AccessType access) const
{
    AccessType access_denied = (access & ~getAccess());
    if (access_denied)
        throw Exception(
            (user_name.empty() ? String() : user_name + ": ") + "Not enough privileges. To run this command you should have been granted "
                + accessToString(access_denied),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void AllowedDatabases::checkAccess(const String & user_name, AccessType access, const String & database) const
{
    AccessType access_denied = (access & ~getAccess(database));
    if (access_denied)
        throw Exception(
            (user_name.empty() ? String() : user_name + ": ") + "Not enough privileges. To run this command you should have been granted "
                + accessToString(access_denied, database),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void AllowedDatabases::checkAccess(const String & user_name, AccessType access, const String & database, const String & table) const
{
    AccessType access_denied = (access & ~getAccess(database, table));
    if (access_denied)
        throw Exception(
            (user_name.empty() ? String() : user_name + ": ") + "Not enough privileges. To run this command you should have been granted "
                + accessToString(access_denied, database, table),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void AllowedDatabases::checkAccess(const String & user_name, AccessType access, const String & database, const String & table, const String & column) const
{
    AccessType access_denied = (access & ~getAccess(database, table, column));
    if (access_denied)
        throw Exception(
            (user_name.empty() ? String() : user_name + ": ") + "Not enough privileges. To run this command you should have been granted "
                + accessToString(access_denied, database, table, column),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void AllowedDatabases::checkAccess(const String & user_name, AccessType access, const String & database, const String & table, const Strings & columns) const
{
    AccessType access_denied = (access & ~getAccess(database, table, columns));
    if (access_denied)
        throw Exception(
            (user_name.empty() ? String() : user_name + ": ") + "Not enough privileges. To run this command you should have been granted "
                + accessToString(access_denied, database, table, columns),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


AllowedDatabases & AllowedDatabases::merge(const AllowedDatabases & other)
{
    root.merge(other.root);
    return *this;
}


AllowedDatabases::Infos AllowedDatabases::getInfo() const
{
    Infos result;
    if (root.getGrants())
        result.emplace_back(Info{root.getGrants(), 0, {}, {}, {}});
    if (root.hasChildren())
    {
        for (const auto & [db_name, db_node] : root.getChildren())
        {
            if (db_node.getGrants() || db_node.getPartialRevokes())
                result.emplace_back(Info{db_node.getGrants(), db_node.getPartialRevokes(), db_name, {}, {}});
            if (db_node.hasChildren())
            {
                for (const auto & [table_name, table_node] : db_node.getChildren())
                {
                    if (table_node.getGrants() || table_node.getPartialRevokes())
                        result.emplace_back(Info{table_node.getGrants(), table_node.getPartialRevokes(), db_name, table_name, {}});
                    if (table_node.hasChildren())
                    {
                        for (const auto & [column_name, column_node] : table_node.getChildren())
                        {
                            if (column_node.getGrants() || column_node.getPartialRevokes())
                                result.emplace_back(Info{column_node.getGrants(), column_node.getPartialRevokes(), db_name, table_name, column_name});
                        }
                    }
                }
            }
        }
    }
    return result;
}


bool operator ==(const AllowedDatabases & left, const AllowedDatabases & right)
{
    return left.root == right.root;
}
}
