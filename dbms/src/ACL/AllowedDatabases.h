#pragma once

#include <Core/Types.h>
#include <memory>
#include <unordered_map>


namespace DB
{

/// Represents a set of databases, tables and columns which a user has access to.
class AllowedDatabases
{
public:
    /// Access types.
    using AccessType = int;
    static const AccessType USAGE;  /// No privileges.
    static const AccessType SELECT; /// User can select data from tables.
    static const AccessType INSERT; /// User can insert data to tables (via either INSERT or ATTACH PARTITION).
    static const AccessType DELETE; /// User can delete data from tables (via either DETACH PARTITION or DROP PARTITION or TRUNCATE).
    static const AccessType ALTER;  /// User can alter tables.
    static const AccessType CREATE; /// User can create databases and tables and attach tables.
    static const AccessType DROP;   /// User can drop tables / databases and detach tables.

    /// Outputs a grant to string in readable format, for example "SELECT(column), INSERT ON mydatabase.*".
    static String accessTypeToString(AccessType access);
    static String accessToString(AccessType access);
    static String accessToString(AccessType access, const String & database);
    static String accessToString(AccessType access, const String & database, const String & table);
    static String accessToString(AccessType access, const String & database, const String & table, const String & column);
    static String accessToString(AccessType access, const String & database, const String & table, const Strings & columns);

    AllowedDatabases();
    ~AllowedDatabases();
    AllowedDatabases(const AllowedDatabases & src);
    AllowedDatabases & operator =(const AllowedDatabases & src);
    AllowedDatabases(AllowedDatabases && src);
    AllowedDatabases & operator =(AllowedDatabases && src);

    /// Returns true if these privileges don't grant anything.
    bool isEmpty() const;

    /// Removes all the privileges.
    void clear();

    /// Grants privileges.
    /// Returns false if the specified grant already exists.
    bool grant(AccessType access);
    bool grant(AccessType access, const String & database);
    bool grant(AccessType access, const String & database, const String & table);
    bool grant(AccessType access, const String & database, const String & table, const String & column);
    bool grant(AccessType access, const String & database, const String & table, const Strings & columns);

    /// Revokes privileges.
    /// Returns false if there is nothing to revoke.
    bool revoke(AccessType access);
    bool revoke(AccessType access, const String & database, bool partial_revokes = false);
    bool revoke(AccessType access, const String & database, const String & table, bool partial_revokes = false);
    bool revoke(AccessType access, const String & database, const String & table, const String & column, bool partial_revokes = false);
    bool revoke(AccessType access, const String & database, const String & table, const Strings & columns, bool partial_revokes = false);

    /// Returns granted privileges for a specified object.
    AccessType getAccess() const { return root.getAccess(); }
    AccessType getAccess(const String & database) const { return root.getAccess(database); }
    AccessType getAccess(const String & database, const String & table) const { return root.getAccess(database, table); }
    AccessType getAccess(const String & database, const String & table, const String & column) const { return root.getAccess(database, table, column); }
    AccessType getAccess(const String & database, const String & table, const Strings & columns) const { return root.getAccess(database, table, columns); }

    /// Checks if a specified privilege is granted. Returns false if it isn't.
    bool hasAccess(AccessType access) const { return !(access & ~getAccess()); }
    bool hasAccess(AccessType access, const String & database) const { return !(access & getAccess(database)); }
    bool hasAccess(AccessType access, const String & database, const String & table) const { return !(access & getAccess(database, table)); }
    bool hasAccess(AccessType access, const String & database, const String & table, const String & column) const { return !(access & getAccess(database, table, column)); }
    bool hasAccess(AccessType access, const String & database, const String & table, const Strings & columns) const { return !(access & getAccess(database, table, columns)); }

    /// Checks if a specified privilege is granted. Throws an exception if it isn't.
    /// `username` is only used for generating an error message if not enough privileges.
    void checkAccess(AccessType access) const;
    void checkAccess(AccessType access, const String & database) const;
    void checkAccess(AccessType access, const String & database, const String & table) const;
    void checkAccess(AccessType access, const String & database, const String & table, const String & column) const;
    void checkAccess(AccessType access, const String & database, const String & table, const Strings & columns) const;
    void checkAccess(const String & user_name, AccessType access) const;
    void checkAccess(const String & user_name, AccessType access, const String & database) const;
    void checkAccess(const String & user_name, AccessType access, const String & database, const String & table) const;
    void checkAccess(const String & user_name, AccessType access, const String & database, const String & table, const String & column) const;
    void checkAccess(const String & user_name, AccessType access, const String & database, const String & table, const Strings & columns) const;

    /// Merges two sets of privileges together.
    /// It's used to combine privileges when several roles are being used by the same user.
    AllowedDatabases & merge(const AllowedDatabases & other);

    struct Info
    {
        AccessType grants = 0;
        AccessType partial_revokes = 0;
        String database; /// If empty that means all databases.
        String table;    /// If empty that means all tables.
        String column;   /// If empty that means all columns.
    };

    using Infos = std::vector<Info>;

    /// Returns the information about the privileges.
    Infos getInfo() const;

    friend bool operator ==(const AllowedDatabases & left, const AllowedDatabases & right);
    friend bool operator !=(const AllowedDatabases & left, const AllowedDatabases & right) { return !(left == right); }

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

        AccessType getAccess() const { return access; }
        AccessType getAccess(const String & name) const;
        AccessType getAccess(const Strings & names) const;
        AccessType getAccess(const String & name1, const String & name2) const;
        AccessType getAccess(const String & name1, const Strings & names2) const;
        AccessType getAccess(const String & name1, const String & name2, const String & name3) const;
        AccessType getAccess(const String & name1, const String & name2, const Strings & names3) const;

        AccessType getParentAccess() const { return parent ? parent->access : 0; }
        AccessType getGrants() const { return grants; }
        AccessType getPartialRevokes() const { return getParentAccess() & ~access; }

        bool grant(AccessType add_access);
        bool grant(AccessType add_access, const String & name);
        bool grant(AccessType add_access, const Strings & names);
        bool grant(AccessType add_access, const String & name1, const String & name2);
        bool grant(AccessType add_access, const String & name1, const Strings & names2);
        bool grant(AccessType add_access, const String & name1, const String & name2, const String & name3);
        bool grant(AccessType add_access, const String & name1, const String & name2, const Strings & names3);

        bool revoke(AccessType remove_access, bool partial_revokes = false);
        bool revoke(AccessType remove_access, const String & name, bool partial_revokes = false);
        bool revoke(AccessType remove_access, const Strings & names, bool partial_revokes = false);
        bool revoke(AccessType remove_access, const String & name1, const String & name2, bool partial_revokes = false);
        bool revoke(AccessType remove_access, const String & name1, const Strings & names2, bool partial_revokes = false);
        bool revoke(AccessType remove_access, const String & name1, const String & name2, const String & name3, bool partial_revokes = false);
        bool revoke(AccessType remove_access, const String & name1, const String & name2, const Strings & names3, bool partial_revokes = false);

        void merge(const Node & other);

        bool operator ==(const Node & other) const;
        bool operator !=(const Node & other) const { return !(*this == other); }

    private:
        void addAccess(AccessType add_access);
        void addAccessRecalcGrants(AccessType add_access);
        void removeAccess(AccessType remove_access);

        template <typename ChildrenMapT = ChildrenMap>
        typename ChildrenMapT::iterator getIterator(const String & child_name);

        template <typename ChildrenMapT = ChildrenMap>
        void eraseOrIncrement(typename ChildrenMapT::iterator & it);

        AccessType access = 0;
        AccessType grants = 0;
        Node * parent = nullptr;
        std::unique_ptr<ChildrenMap> children;
    };

    Node root;
};

}
