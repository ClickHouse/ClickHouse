#include <Access/AccessPrivileges.h>
#include <Common/Exception.h>
#include <Parsers/ASTGrantQuery.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_ENOUGH_PRIVILEGES;
    extern const int INVALID_GRANT;
    extern const int LOGICAL_ERROR;
}


String AccessPrivileges::typeToString(Type type_)
{
    String str;
    for (const auto & [type, name] : getAllTypeNames())
    {
        if (type_ & type)
        {
            str += (str.empty() ? "" : ", ") + name;
            type_ &= ~type;
        }
    }
    if (type_)
        throw Exception("Unknown access type: " + std::to_string(type_), ErrorCodes::LOGICAL_ERROR);
    if (str.empty())
        str = "USAGE";
    return str;
}


const std::vector<std::pair<AccessPrivileges::Type, String>> & AccessPrivileges::getAllTypeNames()
{
    static const std::vector<std::pair<Type, String>> result = []
    {
        return std::vector<std::pair<Type, String>>
        {
            {USAGE, "USAGE"},
            {SELECT, "SELECT"},
            {INSERT, "INSERT"},
            {DELETE, "DELETE"},
            {ALTER, "ALTER"},
            {CREATE, "CREATE"},
            {DROP, "DROP"},
            {CREATE_USER, "CREATE_USER"},
            {ALL, "ALL"},
        };
    }();
    return result;
}


String AccessPrivileges::accessToString(Type access)
{
    return typeToString(access) + " ON *.*";
}


String AccessPrivileges::accessToString(Type access, const String & database)
{
    return typeToString(access) + " ON " + backQuoteIfNeed(database) + ".*";
}


String AccessPrivileges::accessToString(Type access, const String & database, const String & table)
{
    return typeToString(access) + " ON " + backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
}


String AccessPrivileges::accessToString(Type access, const String & database, const String & table, const String & column)
{
    String str;
    for (const auto & [type, name] : getAllTypeNames())
    {
        if (access & type)
        {
            if (!str.empty())
                str += ",";
            str += name + "(" + backQuoteIfNeed(column) + ")";
            access &= ~type;
        }
    }
    if (access)
        throw Exception("Unknown access type: " + std::to_string(access), ErrorCodes::LOGICAL_ERROR);
    if (str.empty())
        str = "USAGE";
    return str + " ON " + backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
}


String AccessPrivileges::accessToString(Type access, const String & database, const String & table, const Strings & columns)
{
    String columns_as_str;
    for (size_t i = 0; i != columns.size(); ++i)
    {
        if (i)
            columns_as_str += ",";
        columns_as_str += backQuoteIfNeed(columns[i]);
    }
    String str;
    for (const auto & [type, name] : getAllTypeNames())
    {
        if (access & type)
        {
            if (!str.empty())
                str += ",";
            str += name + "(" + columns_as_str + ")";
            access &= ~type;
        }
    }
    if (access)
        throw Exception("Unknown access type: " + std::to_string(access), ErrorCodes::LOGICAL_ERROR);
    if (str.empty())
        str = "USAGE";
    return str + " ON " + backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
}


AccessPrivileges::Node::Node(Node && src) { *this = src; }
AccessPrivileges::Node & AccessPrivileges::Node::operator =(Node && src)
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


AccessPrivileges::Node::Node(const Node & src) { *this = src; }
AccessPrivileges::Node & AccessPrivileges::Node::operator =(const Node & src)
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


AccessPrivileges::Node * AccessPrivileges::Node::find(const String & child_name)
{
    if (!children)
        return nullptr;
    auto it = children->find(child_name);
    if (it != children->end())
        return &it->second;
    return nullptr;
}


const AccessPrivileges::Node * AccessPrivileges::Node::find(const String & child_name) const
{
    if (!children)
        return nullptr;
    auto it = children->find(child_name);
    if (it != children->end())
        return &it->second;
    return nullptr;
}


template <typename ChildrenMapT>
typename ChildrenMapT::iterator AccessPrivileges::Node::getIterator(const String & child_name)
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


AccessPrivileges::Node & AccessPrivileges::Node::get(const String & child_name)
{
    return getIterator(child_name)->second;
}


AccessPrivileges::Type AccessPrivileges::Node::getAccess(const String & name) const
{
    const Node * child = find(name);
    return child ? child->access : access;
}


AccessPrivileges::Type AccessPrivileges::Node::getAccess(const Strings & names) const
{
    if (names.empty())
        return 0;
    Type result = getAccess(names[0]);
    for (size_t i = 1; i != names.size(); ++i)
        result &= getAccess(names[i]);
    return result;
}


AccessPrivileges::Type AccessPrivileges::Node::getAccess(const String & name1, const String & name2) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(name2) : access;
}


AccessPrivileges::Type AccessPrivileges::Node::getAccess(const String & name1, const Strings & names2) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(names2) : access;
}


AccessPrivileges::Type AccessPrivileges::Node::getAccess(const String & name1, const String & name2, const String & name3) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(name2, name3) : access;
}


AccessPrivileges::Type AccessPrivileges::Node::getAccess(const String & name1, const String & name2, const Strings & names3) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(name2, names3) : access;
}


bool AccessPrivileges::Node::grant(Type add_access)
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


void AccessPrivileges::Node::addAccess(Type add_access)
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


bool AccessPrivileges::Node::grant(Type add_access, const String & name)
{
    auto it = getIterator(name);
    if (!it->second.grant(add_access))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AccessPrivileges::Node::grant(Type add_access, const Strings & names)
{
    bool changed = false;
    for (const String & name : names)
        changed |= grant(add_access, name);
    return changed;
}


bool AccessPrivileges::Node::grant(Type add_access, const String & name1, const String & name2)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, name2))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AccessPrivileges::Node::grant(Type add_access, const String & name1, const Strings & names2)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, names2))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AccessPrivileges::Node::grant(Type add_access, const String & name1, const String & name2, const String & name3)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, name2, name3))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AccessPrivileges::Node::grant(Type add_access, const String & name1, const String & name2, const Strings & names3)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, name2, names3))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AccessPrivileges::Node::revoke(Type remove_access, bool partial_revokes)
{
    if (partial_revokes)
        remove_access &= access; /// Skip access types we don't have.
    else
        remove_access &= grants; /// Skip access types which are not granted.

    if (!remove_access)
        return false; /// Nothing to revoke.

    /// If (remove_access & ~grants) != 0 then it's a partial revoke.
    /// Partial revokes are implemented like https://dev.mysql.com/doc/refman/8.0/en/partial-revokes.html
    Type new_partial_revokes = remove_access & ~grants;

    grants &= ~remove_access;

    /// Change access types for the children.
    /// We don't change access if it's granted at parent level too.
    removeAccess((remove_access & ~getParentAccess()) | new_partial_revokes);
    return true;
}


void AccessPrivileges::Node::removeAccess(Type remove_access)
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


bool AccessPrivileges::Node::revoke(Type add_access, const String & name, bool partial_revokes)
{
    auto it = getIterator(name);
    if (!it->second.revoke(add_access, partial_revokes))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AccessPrivileges::Node::revoke(Type add_access, const Strings & names, bool partial_revokes)
{
    bool changed = false;
    for (const String & name : names)
        changed |= revoke(add_access, name, partial_revokes);
    return changed;
}


bool AccessPrivileges::Node::revoke(Type add_access, const String & name1, const String & name2, bool partial_revokes)
{
    auto it = getIterator(name1);
    if (!it->second.revoke(add_access, name2, partial_revokes))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AccessPrivileges::Node::revoke(Type add_access, const String & name1, const Strings & names2, bool partial_revokes)
{
    auto it = getIterator(name1);
    if (!it->second.revoke(add_access, names2, partial_revokes))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AccessPrivileges::Node::revoke(Type add_access, const String & name1, const String & name2, const String & name3, bool partial_revokes)
{
    auto it = getIterator(name1);
    if (!it->second.revoke(add_access, name2, name3, partial_revokes))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AccessPrivileges::Node::revoke(Type add_access, const String & name1, const String & name2, const Strings & names3, bool partial_revokes)
{
    auto it = getIterator(name1);
    if (!it->second.revoke(add_access, name2, names3, partial_revokes))
        return false;
    eraseOrIncrement(it);
    return true;
}


template <typename ChildrenMapT>
void AccessPrivileges::Node::eraseOrIncrement(typename ChildrenMapT::iterator & it)
{
    auto & child = it->second;
    if (!child.children && !child.grants && (access == child.access))
        it = children->erase(it);
    else
        ++it;
}


void AccessPrivileges::Node::merge(const Node & other)
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


void AccessPrivileges::Node::addAccessRecalcGrants(Type add_access)
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


bool AccessPrivileges::Node::operator ==(const AccessPrivileges::Node & other) const
{
    if ((access != other.access) || (grants != other.grants))
        return false;
    if (!!children != !!other.children)
        return false;
    if (children && (*children != *other.children))
        return false;
    return true;
}


AccessPrivileges::AccessPrivileges() = default;
AccessPrivileges::~AccessPrivileges() = default;
AccessPrivileges::AccessPrivileges(const AccessPrivileges & src) = default;
AccessPrivileges & AccessPrivileges::operator =(const AccessPrivileges & src) = default;
AccessPrivileges::AccessPrivileges(AccessPrivileges && src) = default;
AccessPrivileges & AccessPrivileges::operator =(AccessPrivileges && src) = default;


bool AccessPrivileges::isEmpty() const
{
    return root.isEmpty();
}


void AccessPrivileges::clear()
{
    root = Node{};
}


bool AccessPrivileges::grant(Type access)
{
    if (access & ~ALL)
        throw Exception("Unknown access type: " + std::to_string(access & ~ALL), ErrorCodes::LOGICAL_ERROR);
    return root.grant(access);
}


bool AccessPrivileges::grant(Type access, const String & database)
{
    if (access & ~ALL_DATABASE_LEVEL)
    {
        if (access & ~ALL)
            throw Exception("Unknown access type: " + std::to_string(access & ~ALL), ErrorCodes::LOGICAL_ERROR);
        throw Exception(
            "Access type " + typeToString(access & ~ALL_DATABASE_LEVEL) + " cannot be granted on a database", ErrorCodes::INVALID_GRANT);
    }
    return root.grant(access, database);
}


bool AccessPrivileges::grant(Type access, const String & database, const String & table)
{
    if (access & ~ALL_TABLE_LEVEL)
    {
        if (access & ~ALL)
            throw Exception("Unknown access type: " + std::to_string(access & ~ALL), ErrorCodes::LOGICAL_ERROR);
        throw Exception(
            "Access type " + typeToString(access & ~ALL_TABLE_LEVEL) + " cannot be granted on a table", ErrorCodes::INVALID_GRANT);
    }
    return root.grant(access, database, table);
}


bool AccessPrivileges::grant(Type access, const String & database, const String & table, const String & column)
{
    if (access & ~ALL_COLUMN_LEVEL)
    {
        if (access & ~ALL)
            throw Exception("Unknown access type: " + std::to_string(access & ~ALL), ErrorCodes::LOGICAL_ERROR);
        throw Exception(
            "Access type " + typeToString(access & ~ALL_COLUMN_LEVEL) + " cannot be granted on a column", ErrorCodes::INVALID_GRANT);
    }
    return root.grant(access, database, table, column);
}


bool AccessPrivileges::grant(Type access, const String & database, const String & table, const Strings & columns)
{
    if (access & ~ALL_COLUMN_LEVEL)
    {
        if (access & ~ALL)
            throw Exception("Unknown access type: " + std::to_string(access & ~ALL), ErrorCodes::LOGICAL_ERROR);
        throw Exception(
            "Access type " + typeToString(access & ~ALL_COLUMN_LEVEL) + " cannot be granted on a column", ErrorCodes::INVALID_GRANT);
    }
    return root.grant(access, database, table, columns);
}


bool AccessPrivileges::revoke(Type access, bool)
{
    return root.revoke(access);
}


bool AccessPrivileges::revoke(Type access, const String & database, bool partial_revokes)
{
    return root.revoke(access, database, partial_revokes);
}


bool AccessPrivileges::revoke(Type access, const String & database, const String & table, bool partial_revokes)
{
    return root.revoke(access, database, table, partial_revokes);
}


bool AccessPrivileges::revoke(Type access, const String & database, const String & table, const String & column, bool partial_revokes)
{
    return root.revoke(access, database, table, column, partial_revokes);
}


bool AccessPrivileges::revoke(Type access, const String & database, const String & table, const Strings & columns, bool partial_revokes)
{
    return root.revoke(access, database, table, columns, partial_revokes);
}


void AccessPrivileges::checkAccess(Type access) const
{
    checkAccess(String(), access);
}


void AccessPrivileges::checkAccess(Type access, const String & database) const
{
    checkAccess(String(), access, database);
}


void AccessPrivileges::checkAccess(Type access, const String & database, const String & table) const
{
    checkAccess(String(), access, database, table);
}


void AccessPrivileges::checkAccess(Type access, const String & database, const String & table, const String & column) const
{
    checkAccess(String(), access, database, table, column);
}


void AccessPrivileges::checkAccess(Type access, const String & database, const String & table, const Strings & columns) const
{
    checkAccess(String(), access, database, table, columns);
}


void AccessPrivileges::checkAccess(const String & user_name, Type access) const
{
    Type not_enough_privileges = (access & ~getAccess());
    if (not_enough_privileges)
        throw Exception(
            (user_name.empty() ? String() : user_name + ": ") + "Not enough privileges. To run this command you should have been granted "
                + accessToString(not_enough_privileges),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void AccessPrivileges::checkAccess(const String & user_name, Type type, const String & database) const
{
    Type not_enough_privileges = (type & ~getAccess(database));
    if (not_enough_privileges)
        throw Exception(
            (user_name.empty() ? String() : user_name + ": ") + "Not enough privileges. To run this command you should have been granted "
                + accessToString(not_enough_privileges, database),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void AccessPrivileges::checkAccess(const String & user_name, Type type, const String & database, const String & table) const
{
    Type not_enough_privileges = (type & ~getAccess(database, table));
    if (not_enough_privileges)
        throw Exception(
            (user_name.empty() ? String() : user_name + ": ") + "Not enough privileges. To run this command you should have been granted "
                + accessToString(not_enough_privileges, database, table),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void AccessPrivileges::checkAccess(const String & user_name, Type access, const String & database, const String & table, const String & column) const
{
    Type not_enough_privileges = (access & ~getAccess(database, table, column));
    if (not_enough_privileges)
        throw Exception(
            (user_name.empty() ? String() : user_name + ": ") + "Not enough privileges. To run this command you should have been granted "
                + accessToString(not_enough_privileges, database, table, column),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void AccessPrivileges::checkAccess(const String & user_name, Type access, const String & database, const String & table, const Strings & columns) const
{
    Type not_enough_privileges = (access & ~getAccess(database, table, columns));
    if (not_enough_privileges)
        throw Exception(
            (user_name.empty() ? String() : user_name + ": ") + "Not enough privileges. To run this command you should have been granted "
                + accessToString(not_enough_privileges, database, table, columns),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


AccessPrivileges & AccessPrivileges::merge(const AccessPrivileges & other)
{
    root.merge(other.root);
    return *this;
}


AccessPrivileges::Infos AccessPrivileges::getInfo() const
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


bool operator ==(const AccessPrivileges & left, const AccessPrivileges & right)
{
    return left.root == right.root;
}
}
