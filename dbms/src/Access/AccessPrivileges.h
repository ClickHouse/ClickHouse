#pragma once

#include <Core/Types.h>
#include <memory>
#include <unordered_map>


namespace DB
{

/// Represents a set of databases, tables and columns which a user has access to.
class AccessPrivileges
{
public:
    /// Access types.
    using Type = int;
    enum Types : Type
    {
        USAGE = 0x00, /// No privileges.
        SELECT = 0x01, /// User can select data from tables.
        INSERT = 0x02, /// User can insert data to tables (via either INSERT or ATTACH PARTITION).
        DELETE = 0x04, /// User can delete data from tables (via either DETACH PARTITION or DROP PARTITION or TRUNCATE).
        ALTER = 0x08, /// User can alter tables.
        CREATE = 0x10, /// User can create databases and tables and attach tables.
        DROP = 0x20, /// User can drop tables / databases and detach tables.
        CREATE_USER = 0x40, /// User can create, alter and drop users and roles.

        ALL_COLUMN_LEVEL = SELECT,
        ALL_TABLE_LEVEL = ALL_COLUMN_LEVEL | INSERT | DELETE | ALTER | DROP,
        ALL_DATABASE_LEVEL = ALL_TABLE_LEVEL | CREATE,
        ALL_GLOBAL_LEVEL = ALL_DATABASE_LEVEL | CREATE_USER,
        ALL = ALL_GLOBAL_LEVEL,
    };

    /// Outputs a grant to string in readable format, for example "SELECT(column), INSERT ON mydatabase.*".
    static String typeToString(Type type);
    static const std::vector<std::pair<Type, String>> & getAllTypeNames();
    static String accessToString(Type access);
    static String accessToString(Type access, const String & database);
    static String accessToString(Type access, const String & database, const String & table);
    static String accessToString(Type access, const String & database, const String & table, const String & column);
    static String accessToString(Type access, const String & database, const String & table, const Strings & columns);

    AccessPrivileges();
    ~AccessPrivileges();
    AccessPrivileges(const AccessPrivileges & src);
    AccessPrivileges & operator =(const AccessPrivileges & src);
    AccessPrivileges(AccessPrivileges && src);
    AccessPrivileges & operator =(AccessPrivileges && src);

    /// Returns true if these privileges don't grant anything.
    bool isEmpty() const;

    /// Removes all the privileges.
    void clear();

    /// Grants privileges.
    /// Returns false if the specified grant already exists.
    bool grant(Type access);
    bool grant(Type access, const String & database);
    bool grant(Type access, const String & database, const String & table);
    bool grant(Type access, const String & database, const String & table, const String & column);
    bool grant(Type access, const String & database, const String & table, const Strings & columns);

    /// Revokes privileges.
    /// Returns false if there is nothing to revoke.
    bool revoke(Type access);
    bool revoke(Type access, const String & database, bool partial_revokes = false);
    bool revoke(Type access, const String & database, const String & table, bool partial_revokes = false);
    bool revoke(Type access, const String & database, const String & table, const String & column, bool partial_revokes = false);
    bool revoke(Type access, const String & database, const String & table, const Strings & columns, bool partial_revokes = false);

    /// Returns granted privileges for a specified object.
    Type getAccess() const { return root.getAccess(); }
    Type getAccess(const String & database) const { return root.getAccess(database); }
    Type getAccess(const String & database, const String & table) const { return root.getAccess(database, table); }
    Type getAccess(const String & database, const String & table, const String & column) const { return root.getAccess(database, table, column); }
    Type getAccess(const String & database, const String & table, const Strings & columns) const { return root.getAccess(database, table, columns); }

    /// Checks if a specified privilege is granted. Returns false if it isn't.
    bool hasAccess(Type access) const { return !(access & ~getAccess()); }
    bool hasAccess(Type access, const String & database) const { return !(access & getAccess(database)); }
    bool hasAccess(Type access, const String & database, const String & table) const { return !(access & getAccess(database, table)); }
    bool hasAccess(Type access, const String & database, const String & table, const String & column) const { return !(access & getAccess(database, table, column)); }
    bool hasAccess(Type access, const String & database, const String & table, const Strings & columns) const { return !(access & getAccess(database, table, columns)); }

    /// Checks if a specified privilege is granted. Throws an exception if it isn't.
    /// `username` is only used for generating an error message if not enough privileges.
    void checkAccess(Type access) const;
    void checkAccess(Type access, const String & database) const;
    void checkAccess(Type access, const String & database, const String & table) const;
    void checkAccess(Type access, const String & database, const String & table, const String & column) const;
    void checkAccess(Type access, const String & database, const String & table, const Strings & columns) const;
    void checkAccess(const String & user_name, Type access) const;
    void checkAccess(const String & user_name, Type access, const String & database) const;
    void checkAccess(const String & user_name, Type access, const String & database, const String & table) const;
    void checkAccess(const String & user_name, Type access, const String & database, const String & table, const String & column) const;
    void checkAccess(const String & user_name, Type access, const String & database, const String & table, const Strings & columns) const;

    /// Merges two sets of privileges together.
    /// It's used to combine privileges when several roles are being used by the same user.
    AccessPrivileges & merge(const AccessPrivileges & other);

    struct Info
    {
        Type grants = 0;
        Type partial_revokes = 0;
        String database; /// If empty that means all databases.
        String table;    /// If empty that means all tables.
        String column;   /// If empty that means all columns.
    };

    using Infos = std::vector<Info>;

    /// Returns the information about the privileges.
    Infos getInfo() const;

    friend bool operator ==(const AccessPrivileges & left, const AccessPrivileges & right);
    friend bool operator !=(const AccessPrivileges & left, const AccessPrivileges & right) { return !(left == right); }

private:
    class Node
    {
    public:
        Node() = default;
        Node(Node && src);
        Node & operator =(Node && src);
        Node(const Node & src);
        Node & operator =(const Node & src);

        bool isEmpty() const { return !getGrants() && !getPartialRevokes() && !hasChildren(); }
        bool hasChildren() const { return children.get(); }

        using ChildrenMap = std::unordered_map<String, Node>;
        const ChildrenMap & getChildren() const { return *children; }

        Node * find(const String & child_name);
        const Node * find(const String & child_name) const;
        Node & get(const String & child_name);

        Type getAccess() const { return access; }
        Type getAccess(const String & name) const;
        Type getAccess(const Strings & names) const;
        Type getAccess(const String & name1, const String & name2) const;
        Type getAccess(const String & name1, const Strings & names2) const;
        Type getAccess(const String & name1, const String & name2, const String & name3) const;
        Type getAccess(const String & name1, const String & name2, const Strings & names3) const;

        Type getParentAccess() const { return parent ? parent->access : 0; }
        Type getGrants() const { return grants; }
        Type getPartialRevokes() const { return getParentAccess() & ~access; }

        bool grant(Type add_access);
        bool grant(Type add_access, const String & name);
        bool grant(Type add_access, const Strings & names);
        bool grant(Type add_access, const String & name1, const String & name2);
        bool grant(Type add_access, const String & name1, const Strings & names2);
        bool grant(Type add_access, const String & name1, const String & name2, const String & name3);
        bool grant(Type add_access, const String & name1, const String & name2, const Strings & names3);

        bool revoke(Type remove_access, bool partial_revokes = false);
        bool revoke(Type remove_access, const String & name, bool partial_revokes = false);
        bool revoke(Type remove_access, const Strings & names, bool partial_revokes = false);
        bool revoke(Type remove_access, const String & name1, const String & name2, bool partial_revokes = false);
        bool revoke(Type remove_access, const String & name1, const Strings & names2, bool partial_revokes = false);
        bool revoke(Type remove_access, const String & name1, const String & name2, const String & name3, bool partial_revokes = false);
        bool revoke(Type remove_access, const String & name1, const String & name2, const Strings & names3, bool partial_revokes = false);

        void merge(const Node & other);

        bool operator ==(const Node & other) const;
        bool operator !=(const Node & other) const { return !(*this == other); }

    private:
        void addAccess(Type add_access);
        void addAccessRecalcGrants(Type add_access);
        void removeAccess(Type remove_access);

        template <typename ChildrenMapT = ChildrenMap>
        typename ChildrenMapT::iterator getIterator(const String & child_name);

        template <typename ChildrenMapT = ChildrenMap>
        void eraseOrIncrement(typename ChildrenMapT::iterator & it);

        Type access = 0;
        Type grants = 0;
        Node * parent = nullptr;
        std::unique_ptr<ChildrenMap> children;
    };

    Node root;
};

}
