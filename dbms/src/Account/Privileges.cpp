#include <Account/Privileges.h>
#include <Common/Exception.h>
#include <Parsers/IAST.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_GRANT;
    extern const int NOT_ENOUGH_PRIVILEGES;
}


Privileges::Node::Node(Node && src) { *this = src; }
Privileges::Node & Privileges::Node::operator =(Node && src)
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


Privileges::Node::Node(const Node & src) { *this = src; }
Privileges::Node & Privileges::Node::operator =(const Node & src)
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


Privileges::Node * Privileges::Node::find(const String & child_name)
{
    if (!children)
        return nullptr;
    auto it = children->find(child_name);
    if (it != children->end())
        return &it->second;
    return nullptr;
}


const Privileges::Node * Privileges::Node::find(const String & child_name) const
{
    if (!children)
        return nullptr;
    auto it = children->find(child_name);
    if (it != children->end())
        return &it->second;
    return nullptr;
}


template <typename ChildrenMapT>
typename ChildrenMapT::iterator Privileges::Node::getIterator(const String & child_name)
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


Privileges::Node & Privileges::Node::get(const String & child_name)
{
    return getIterator(child_name)->second;
}


Privileges::Types Privileges::Node::getAccess(const String & name) const
{
    const Node * child = find(name);
    return child ? child->access : access;
}


Privileges::Types Privileges::Node::getAccess(const Strings & names) const
{
    Types result = ALL_PRIVILEGES;
    for (const auto & name : names)
        result &= getAccess(name);
    return result;
}


Privileges::Types Privileges::Node::getAccess(const String & name1, const String & name2) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(name2) : access;
}


Privileges::Types Privileges::Node::getAccess(const String & name1, const Strings & names2) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(names2) : access;
}


Privileges::Types Privileges::Node::getAccess(const String & name1, const String & name2, const String & name3) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(name2, name3) : access;
}


Privileges::Types Privileges::Node::getAccess(const String & name1, const String & name2, const Strings & names3) const
{
    const Node * child = find(name1);
    return child ? child->getAccess(name2, names3) : access;
}


bool Privileges::Node::grant(Types add_access)
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


void Privileges::Node::addAccess(Types add_access)
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


bool Privileges::Node::grant(Types add_access, const String & name)
{
    auto it = getIterator(name);
    if (!it->second.grant(add_access))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool Privileges::Node::grant(Types add_access, const Strings & names)
{
    bool changed = false;
    for (const String & name : names)
        changed |= grant(add_access, name);
    return changed;
}


bool Privileges::Node::grant(Types add_access, const String & name1, const String & name2)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, name2))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool Privileges::Node::grant(Types add_access, const String & name1, const Strings & names2)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, names2))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool Privileges::Node::grant(Types add_access, const String & name1, const String & name2, const String & name3)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, name2, name3))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool Privileges::Node::grant(Types add_access, const String & name1, const String & name2, const Strings & names3)
{
    auto it = getIterator(name1);
    if (!it->second.grant(add_access, name2, names3))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool Privileges::Node::revoke(Types remove_access)
{
    remove_access &= access; /// Skip access types which we don't have.
    if (!remove_access)
        return false; /// Nothing to revoke.

    /// If (remove_access & ~grants) != 0 then it's a partial revoke.
    /// Partial revokes are implemented like https://dev.mysql.com/doc/refman/8.0/en/partial-revokes.html
    Types new_partial_revokes = remove_access & ~grants;

    grants &= ~remove_access;

    /// Change access types for the children.
    /// We don't change access if it's granted at parent level too.
    removeAccess((remove_access & ~getParentAccess()) | new_partial_revokes);
    return true;
}


void Privileges::Node::removeAccess(Types remove_access)
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


bool Privileges::Node::revoke(Types add_access, const String & name)
{
    auto it = getIterator(name);
    if (!it->second.revoke(add_access))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool Privileges::Node::revoke(Types add_access, const Strings & names)
{
    bool changed = false;
    for (const String & name : names)
        changed |= revoke(add_access, name);
    return changed;
}


bool Privileges::Node::revoke(Types add_access, const String & name1, const String & name2)
{
    auto it = getIterator(name1);
    if (!it->second.revoke(add_access, name2))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool Privileges::Node::revoke(Types add_access, const String & name1, const Strings & names2)
{
    auto it = getIterator(name1);
    if (!it->second.revoke(add_access, names2))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool Privileges::Node::revoke(Types add_access, const String & name1, const String & name2, const String & name3)
{
    auto it = getIterator(name1);
    if (!it->second.revoke(add_access, name2, name3))
        return false;
    eraseOrIncrement(it);
    return true;
}


bool Privileges::Node::revoke(Types add_access, const String & name1, const String & name2, const Strings & names3)
{
    auto it = getIterator(name1);
    if (!it->second.revoke(add_access, name2, names3))
        return false;
    eraseOrIncrement(it);
    return true;
}


template <typename ChildrenMapT>
void Privileges::Node::eraseOrIncrement(typename ChildrenMapT::iterator & it)
{
    auto & child = it->second;
    if (!child.children && !child.grants && (access == child.access))
        it = children->erase(it);
    else
        ++it;
}


void Privileges::Node::merge(const Node & other)
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


void Privileges::Node::addAccessRecalcGrants(Types add_access)
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


bool Privileges::Node::operator ==(const Privileges::Node & other) const
{
    if ((access != other.access) || (grants != other.grants))
        return false;
    if (!!children != !!other.children)
        return false;
    if (children && (*children != *other.children))
        return false;
    return true;
}


String Privileges::accessToString(Types access)
{
    struct AccessDesc { Types access; const char * text; };
    static constexpr AccessDesc descs[] =
    {
        { ALL_PRIVILEGES, "ALL PRIVILEGES" },
        { SELECT, "SELECT" },
        { INSERT, "INSERT" },
        { DELETE, "DELETE" },
        { ALTER, "ALTER" },
        { CREATE, "CREATE" },
        { DROP, "DROP" },
        { CREATE_USER, "CREATE USER" },
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


String Privileges::accessToString(Types access, const String & database)
{
    return accessToString(access) + " ON " + backQuoteIfNeed(database) + ".*";
}


String Privileges::accessToString(Types access, const String & database, const String & table)
{
    return accessToString(access) + " ON " + backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
}


String Privileges::accessToString(Types access, const String & database, const String & table, const String & column)
{
    String str;
    access &= ALL_PRIVILEGES;
    for (Types flag = 0x01; access; flag <<= 1)
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


String Privileges::accessToString(Types access, const String & database, const String & table, const Strings & columns)
{
    String str;
    access &= ALL_PRIVILEGES;
    for (Types flag = 0x01; access; flag <<= 1)
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


Privileges::Privileges()
{
    static_assert(!(COLUMN_LEVEL & ~TABLE_LEVEL));
    static_assert(!(TABLE_LEVEL & ~DATABASE_LEVEL));
    static_assert(!(DATABASE_LEVEL & ~GLOBAL_LEVEL));
    static_assert(GLOBAL_LEVEL == ALL_PRIVILEGES);
}


Privileges::~Privileges() = default;
Privileges::Privileges(const Privileges & src) = default;
Privileges & Privileges::operator =(const Privileges & src) = default;
Privileges::Privileges(Privileges && src) = default;
Privileges & Privileges::operator =(Privileges && src) = default;


bool Privileges::isEmpty() const
{
    return root.isEmpty();
}


void Privileges::clear()
{
    root = Node{};
}


bool Privileges::grant(Types access)
{
    access &= ALL_PRIVILEGES;
    return root.grant(access);
}


bool Privileges::grant(Types access, const String & database)
{
    access &= ALL_PRIVILEGES;
    if (access & ~DATABASE_LEVEL)
        throw Exception("The privilege " + accessToString(access & ~DATABASE_LEVEL) + " can't be granted on a database", ErrorCodes::INVALID_GRANT);
    return root.grant(access, database);
}


bool Privileges::grant(Types access, const String & database, const String & table)
{
    access &= ALL_PRIVILEGES;
    if (access & ~TABLE_LEVEL)
        throw Exception("The privilege " + accessToString(access & ~TABLE_LEVEL) + " can't be granted on a table", ErrorCodes::INVALID_GRANT);
    return root.grant(access, database, table);
}


bool Privileges::grant(Types access, const String & database, const String & table, const String & column)
{
    access &= ALL_PRIVILEGES;
    if (access & ~COLUMN_LEVEL)
        throw Exception("The privilege " + accessToString(access & ~COLUMN_LEVEL) + " can't be granted on a columns", ErrorCodes::INVALID_GRANT);
    return root.grant(access, database, table, column);
}


bool Privileges::grant(Types access, const String & database, const String & table, const Strings & columns)
{
    access &= ALL_PRIVILEGES;
    if (access & ~COLUMN_LEVEL)
        throw Exception("The privilege " + accessToString(access & ~COLUMN_LEVEL) + " can't be granted on a columns", ErrorCodes::INVALID_GRANT);
    return root.grant(access, database, table, columns);
}


bool Privileges::revoke(Types access)
{
    return root.revoke(access);
}


bool Privileges::revoke(Types access, const String & database)
{
    return root.revoke(access, database);
}


bool Privileges::revoke(Types access, const String & database, const String & table)
{
    return root.revoke(access, database, table);
}


bool Privileges::revoke(Types access, const String & database, const String & table, const String & column)
{
    return root.revoke(access, database, table, column);
}


bool Privileges::revoke(Types access, const String & database, const String & table, const Strings & columns)
{
    return root.revoke(access, database, table, columns);
}


void Privileges::checkAccess(Types access) const
{
    Types access_denied = (access & ~getAccess());
    if (access_denied)
        throw Exception(
            "Not enough privileges. To run this command you should have been granted " + accessToString(access_denied),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void Privileges::checkAccess(Types access, const String & database) const
{
    Types access_denied = (access & ~getAccess(database));
    if (access_denied)
        throw Exception(
            "Not enough privileges. To run this command you should have been granted " + accessToString(access_denied, database),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void Privileges::checkAccess(Types access, const String & database, const String & table) const
{
    Types access_denied = (access & ~getAccess(database, table));
    if (access_denied)
        throw Exception(
            "Not enough privileges. To run this command you should have been granted " + accessToString(access_denied, database, table),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void Privileges::checkAccess(Types access, const String & database, const String & table, const String & column) const
{
    Types access_denied = (access & ~getAccess(database, table, column));
    if (access_denied)
        throw Exception(
            "Not enough privileges. To run this command you should have been granted " + accessToString(access_denied, database, table, column),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


void Privileges::checkAccess(Types access, const String & database, const String & table, const Strings & columns) const
{
    Types access_denied = (access & ~getAccess(database, table, columns));
    if (access_denied)
        throw Exception(
            "Not enough privileges. To run this command you should have been granted " + accessToString(access_denied, database, table, columns),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}


Privileges & Privileges::merge(const Privileges & other)
{
    root.merge(other.root);
    return *this;
}


std::vector<Privileges::Info> Privileges::getInfo() const
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


bool operator ==(const Privileges & left, const Privileges & right)
{
    return left.root == right.root;
}

}
