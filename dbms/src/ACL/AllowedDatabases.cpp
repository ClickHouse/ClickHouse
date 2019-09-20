#include <ACL/AllowedDatabases.h>
#include <Common/Exception.h>
#include <Parsers/IAST.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_GRANT;
    extern const int NOT_ENOUGH_PRIVILEGES;
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


AllowedDatabases::AccessTypes AllowedDatabases::Node::getAccess(const String & name) const
{
    const Node * child = find(name);
    return child ? child->access : access;
}


AllowedDatabases::AccessTypes AllowedDatabases::Node::getAccess(const Strings & names) const
{
    AccessTypes result = ALL_PRIVILEGES;
    for (const auto & name : names)
        result &= getAccess(name);
    return result;
}


AllowedDatabases::AccessTypes AllowedDatabases::Node::getAccess(const String & name1, const String & name2) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(name2) : access;
}


AllowedDatabases::AccessTypes AllowedDatabases::Node::getAccess(const String & name1, const Strings & names2) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(names2) : access;
}


AllowedDatabases::AccessTypes AllowedDatabases::Node::getAccess(const String & name1, const String & name2, const String & name3) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(name2, name3) : access;
}


AllowedDatabases::AccessTypes AllowedDatabases::Node::getAccess(const String & name1, const String & name2, const Strings & names3) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(name2, names3) : access;
}


bool AllowedDatabases::Node::grant(AccessTypes add_access)
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


void AllowedDatabases::Node::addAccess(AccessTypes add_access)
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


bool AllowedDatabases::Node::grant(AccessTypes add_access, const String & name)
{
    auto it = getIterator(name);
    if (!it->second.grant(add_access))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::grant(AccessTypes add_access, const Strings & names)
{
    bool changed = false;
    for (const String & name : names)
        changed |= grant(add_access, name);
    return changed;
}


bool AllowedDatabases::Node::grant(AccessTypes add_access, const String & name1, const String & name2)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, name2))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::grant(AccessTypes add_access, const String & name1, const Strings & names2)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, names2))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::grant(AccessTypes add_access, const String & name1, const String & name2, const String & name3)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, name2, name3))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::grant(AccessTypes add_access, const String & name1, const String & name2, const Strings & names3)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, name2, names3))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::revoke(AccessTypes remove_access, bool partial_revokes)
{
    if (partial_revokes)
        remove_access &= access; /// Skip access types we don't have.
    else
        remove_access &= grants; /// Skip access types which are not granted.

    if (!remove_access)
        return false; /// Nothing to revoke.

    /// If (remove_access & ~grants) != 0 then it's a partial revoke.
    /// Partial revokes are implemented like https://dev.mysql.com/doc/refman/8.0/en/partial-revokes.html
    AccessTypes new_partial_revokes = remove_access & ~grants;

    grants &= ~remove_access;

    /// Change access types for the children.
    /// We don't change access if it's granted at parent level too.
    removeAccess((remove_access & ~getParentAccess()) | new_partial_revokes);
    return true;
}


void AllowedDatabases::Node::removeAccess(AccessTypes remove_access)
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


bool AllowedDatabases::Node::revoke(AccessTypes add_access, const String & name, bool partial_revokes)
{
    auto it = getIterator(name);
    if (!it->second.revoke(add_access, partial_revokes))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::revoke(AccessTypes add_access, const Strings & names, bool partial_revokes)
{
    bool changed = false;
    for (const String & name : names)
        changed |= revoke(add_access, name, partial_revokes);
    return changed;
}


bool AllowedDatabases::Node::revoke(AccessTypes add_access, const String & name1, const String & name2, bool partial_revokes)
{
    auto it = getIterator(name1);
    if (!it->second.revoke(add_access, name2, partial_revokes))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::revoke(AccessTypes add_access, const String & name1, const Strings & names2, bool partial_revokes)
{
    auto it = getIterator(name1);
    if (!it->second.revoke(add_access, names2, partial_revokes))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::revoke(AccessTypes add_access, const String & name1, const String & name2, const String & name3, bool partial_revokes)
{
    auto it = getIterator(name1);
    if (!it->second.revoke(add_access, name2, name3, partial_revokes))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool AllowedDatabases::Node::revoke(AccessTypes add_access, const String & name1, const String & name2, const Strings & names3, bool partial_revokes)
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


void AllowedDatabases::Node::addAccessRecalcGrants(AccessTypes add_access)
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


String AllowedDatabases::accessToString(AccessTypes access)
{
    struct AccessDesc { AccessTypes access; const char * text; };
    static constexpr AccessDesc descs[] =
    {
        { ALL_PRIVILEGES, "ALL PRIVILEGES" },
        { SELECT, "SELECT" },
        { INSERT, "INSERT" },
        { DELETE, "DELETE" },
        { ALTER, "ALTER" },
        { CREATE, "CREATE" },
        { DROP, "DROP" },
    };

    String str;
    for (size_t i = 0; i != std::size(descs) && access; ++i)
    {
        const auto & desc = descs[i];
        if ((access & desc.access) == desc.access)
        {
            if (!str.empty())
                str += ",";
            str += desc.text;
            access &= ~desc.access;
        }
    }
    if (str.empty())
        str += "USAGE";
    return str;
}


String AllowedDatabases::accessToString(AccessTypes access, const String & database)
{
    return accessToString(access) + " ON " + backQuoteIfNeed(database) + ".*";
}


String AllowedDatabases::accessToString(AccessTypes access, const String & database, const String & table)
{
    return accessToString(access) + " ON " + backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
}


String AllowedDatabases::accessToString(AccessTypes access, const String & database, const String & table, const String & column)
{
    String str;
    access &= ALL_PRIVILEGES;
    for (AccessTypes flag = 0x01; access; flag <<= 1)
    {
        if (access & flag)
        {
            if (!str.empty())
                str += ",";
            str += accessToString(access & flag) + "(" + backQuoteIfNeed(column) + ")";
            access &= ~flag;
        }
    }
    return str + " ON " + backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
}


String AllowedDatabases::accessToString(AccessTypes access, const String & database, const String & table, const Strings & columns)
{
    String str;
    access &= ALL_PRIVILEGES;
    for (AccessTypes flag = 0x01; access; flag <<= 1)
    {
        if (access & flag)
        {
            if (!str.empty())
                str += ",";
            str += accessToString(access & flag) + "(";
            for (size_t i = 0; i != columns.size(); ++i)
            {
                if (i)
                    str += ",";
                str += backQuoteIfNeed(columns[i]);
            }
            str += ")";
            access &= ~flag;
        }
    }
    return str + " ON " + backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
}


AllowedDatabases::AllowedDatabases()
{
    static_assert(!(COLUMN_LEVEL & ~TABLE_LEVEL));
    static_assert(!(TABLE_LEVEL & ~DATABASE_LEVEL));
    static_assert(DATABASE_LEVEL == ALL_PRIVILEGES);
}


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


bool AllowedDatabases::grant(AccessTypes access)
{
    access &= ALL_PRIVILEGES;
    return root.grant(access);
}


bool AllowedDatabases::grant(AccessTypes access, const String & database)
{
    access &= ALL_PRIVILEGES;
    if (access & ~DATABASE_LEVEL)
        throw Exception("The privilege " + accessToString(access & ~DATABASE_LEVEL) + " can't be granted on a database", ErrorCodes::INVALID_GRANT);
    return root.grant(access, database);
}


bool AllowedDatabases::grant(AccessTypes access, const String & database, const String & table)
{
    access &= ALL_PRIVILEGES;
    if (access & ~TABLE_LEVEL)
        throw Exception("The privilege " + accessToString(access & ~TABLE_LEVEL) + " can't be granted on a table", ErrorCodes::INVALID_GRANT);
    return root.grant(access, database, table);
}


bool AllowedDatabases::grant(AccessTypes access, const String & database, const String & table, const String & column)
{
    access &= ALL_PRIVILEGES;
    if (access & ~COLUMN_LEVEL)
        throw Exception("The privilege " + accessToString(access & ~COLUMN_LEVEL) + " can't be granted on a columns", ErrorCodes::INVALID_GRANT);
    return root.grant(access, database, table, column);
}


bool AllowedDatabases::grant(AccessTypes access, const String & database, const String & table, const Strings & columns)
{
    access &= ALL_PRIVILEGES;
    if (access & ~COLUMN_LEVEL)
        throw Exception("The privilege " + accessToString(access & ~COLUMN_LEVEL) + " can't be granted on a columns", ErrorCodes::INVALID_GRANT);
    return root.grant(access, database, table, columns);
}


bool AllowedDatabases::revoke(AccessTypes access)
{
    return root.revoke(access);
}


bool AllowedDatabases::revoke(AccessTypes access, const String & database, bool partial_revokes)
{
    return root.revoke(access, database, partial_revokes);
}


bool AllowedDatabases::revoke(AccessTypes access, const String & database, const String & table, bool partial_revokes)
{
    return root.revoke(access, database, table, partial_revokes);
}


bool AllowedDatabases::revoke(AccessTypes access, const String & database, const String & table, const String & column, bool partial_revokes)
{
    return root.revoke(access, database, table, column, partial_revokes);
}


bool AllowedDatabases::revoke(AccessTypes access, const String & database, const String & table, const Strings & columns, bool partial_revokes)
{
    return root.revoke(access, database, table, columns, partial_revokes);
}


void AllowedDatabases::checkAccess(AccessTypes access) const
{
    checkAccess(String(), access);
}


void AllowedDatabases::checkAccess(AccessTypes access, const String & database) const
{
    checkAccess(String(), access, database);
}


void AllowedDatabases::checkAccess(AccessTypes access, const String & database, const String & table) const
{
    checkAccess(String(), access, database, table);
}


void AllowedDatabases::checkAccess(AccessTypes access, const String & database, const String & table, const String & column) const
{
    checkAccess(String(), access, database, table, column);
}


void AllowedDatabases::checkAccess(AccessTypes access, const String & database, const String & table, const Strings & columns) const
{
    checkAccess(String(), access, database, table, columns);
}


void AllowedDatabases::checkAccess(const String & user_name, AccessTypes access) const
{
    AccessTypes access_denied = (access & ~getAccess());
    if (access_denied)
        throw Exception(
            (user_name.empty() ? String() : user_name + ": ") + "Not enough AllowedDatabases. To run this command you should have been granted "
                + accessToString(access_denied),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void AllowedDatabases::checkAccess(const String & user_name, AccessTypes access, const String & database) const
{
    AccessTypes access_denied = (access & ~getAccess(database));
    if (access_denied)
        throw Exception(
            (user_name.empty() ? String() : user_name + ": ") + "Not enough AllowedDatabases. To run this command you should have been granted "
                + accessToString(access_denied, database),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void AllowedDatabases::checkAccess(const String & user_name, AccessTypes access, const String & database, const String & table) const
{
    AccessTypes access_denied = (access & ~getAccess(database, table));
    if (access_denied)
        throw Exception(
            (user_name.empty() ? String() : user_name + ": ") + "Not enough AllowedDatabases. To run this command you should have been granted "
                + accessToString(access_denied, database, table),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void AllowedDatabases::checkAccess(const String & user_name, AccessTypes access, const String & database, const String & table, const String & column) const
{
    AccessTypes access_denied = (access & ~getAccess(database, table, column));
    if (access_denied)
        throw Exception(
            (user_name.empty() ? String() : user_name + ": ") + "Not enough AllowedDatabases. To run this command you should have been granted "
                + accessToString(access_denied, database, table, column),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void AllowedDatabases::checkAccess(const String & user_name, AccessTypes access, const String & database, const String & table, const Strings & columns) const
{
    AccessTypes access_denied = (access & ~getAccess(database, table, columns));
    if (access_denied)
        throw Exception(
            (user_name.empty() ? String() : user_name + ": ") + "Not enough AllowedDatabases. To run this command you should have been granted "
                + accessToString(access_denied, database, table, columns),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


AllowedDatabases & AllowedDatabases::merge(const AllowedDatabases & other)
{
    root.merge(other.root);
    return *this;
}


std::vector<AllowedDatabases::Info> AllowedDatabases::getInfo() const
{
    std::vector<Info> result;
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
