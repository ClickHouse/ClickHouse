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
    using AccessTypes = int; /// Combination of the constants below:
    enum
    {
        /// No privileges.
        USAGE = 0x00,

        /// User can select data from tables.
        SELECT = 0x01,

        /// User can insert data to tables (via either INSERT or ATTACH PARTITION).
        INSERT = 0x02,

        /// User can delete data from tables (via either DETACH PARTITION or DROP PARTITION or TRUNCATE).
        DELETE = 0x04,

        /// User can alter tables.
        ALTER = 0x08,

        /// User can create databases and tables and attach tables.
        CREATE = 0x10,

        /// User can drop tables / databases and detach tables.
        DROP = 0x20,

        /// All the privileges.
        ALL_PRIVILEGES = 0x3F,
    };

    static constexpr AccessTypes COLUMN_LEVEL = SELECT;
    static constexpr AccessTypes TABLE_LEVEL = COLUMN_LEVEL | INSERT | DELETE | ALTER | DROP;
    static constexpr AccessTypes DATABASE_LEVEL = TABLE_LEVEL | CREATE;

    /// Outputs a grant to string in readable format, for example "SELECT(column), INSERT ON mydatabase.*".
    static String accessToString(AccessTypes access);
    static String accessToString(AccessTypes access, const String & database);
    static String accessToString(AccessTypes access, const String & database, const String & table);
    static String accessToString(AccessTypes access, const String & database, const String & table, const String & column);
    static String accessToString(AccessTypes access, const String & database, const String & table, const Strings & columns);

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
    bool grant(AccessTypes access);
    bool grant(AccessTypes access, const String & database);
    bool grant(AccessTypes access, const String & database, const String & table);
    bool grant(AccessTypes access, const String & database, const String & table, const String & column);
    bool grant(AccessTypes access, const String & database, const String & table, const Strings & columns);

    /// Revokes privileges.
    /// Returns false if there is nothing to revoke.
    bool revoke(AccessTypes access);
    bool revoke(AccessTypes access, const String & database, bool partial_revokes = false);
    bool revoke(AccessTypes access, const String & database, const String & table, bool partial_revokes = false);
    bool revoke(AccessTypes access, const String & database, const String & table, const String & column, bool partial_revokes = false);
    bool revoke(AccessTypes access, const String & database, const String & table, const Strings & columns, bool partial_revokes = false);

    /// Returns granted privileges for a specified object.
    AccessTypes getAccess() const { return root.getAccess(); }
    AccessTypes getAccess(const String & database) const { return root.getAccess(database); }
    AccessTypes getAccess(const String & database, const String & table) const { return root.getAccess(database, table); }
    AccessTypes getAccess(const String & database, const String & table, const String & column) const { return root.getAccess(database, table, column); }
    AccessTypes getAccess(const String & database, const String & table, const Strings & columns) const { return root.getAccess(database, table, columns); }

    /// Checks if a specified privilege is granted. Returns false if it isn't.
    bool hasAccess(AccessTypes access) const { return !(access & ~getAccess()); }
    bool hasAccess(AccessTypes access, const String & database) const { return !(access & getAccess(database)); }
    bool hasAccess(AccessTypes access, const String & database, const String & table) const { return !(access & getAccess(database, table)); }
    bool hasAccess(AccessTypes access, const String & database, const String & table, const String & column) const { return !(access & getAccess(database, table, column)); }
    bool hasAccess(AccessTypes access, const String & database, const String & table, const Strings & columns) const { return !(access & getAccess(database, table, columns)); }

    /// Checks if a specified privilege is granted. Throws an exception if it isn't.
    /// `username` is only used for generating an error message if not enough privileges.
    void checkAccess(AccessTypes access) const;
    void checkAccess(AccessTypes access, const String & database) const;
    void checkAccess(AccessTypes access, const String & database, const String & table) const;
    void checkAccess(AccessTypes access, const String & database, const String & table, const String & column) const;
    void checkAccess(AccessTypes access, const String & database, const String & table, const Strings & columns) const;
    void checkAccess(const String & user_name, AccessTypes access) const;
    void checkAccess(const String & user_name, AccessTypes access, const String & database) const;
    void checkAccess(const String & user_name, AccessTypes access, const String & database, const String & table) const;
    void checkAccess(const String & user_name, AccessTypes access, const String & database, const String & table, const String & column) const;
    void checkAccess(const String & user_name, AccessTypes access, const String & database, const String & table, const Strings & columns) const;

    /// Merges two sets of privileges together.
    /// It's used to combine privileges when several roles are being used by the same user.
    AllowedDatabases & merge(const AllowedDatabases & other);

    struct Info
    {
        AccessTypes grants;
        AccessTypes partial_revokes;
        std::optional<String> database; /// If not set that means all databases.
        std::optional<String> table;    /// If not set that means all tables.
        std::optional<String> column;   /// If not set that means all columns.
    };

    /// Returns the information about the privileges.
    std::vector<Info> getInfo() const;

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

        AccessTypes getAccess() const { return access; }
        AccessTypes getAccess(const String & name) const;
        AccessTypes getAccess(const Strings & names) const;
        AccessTypes getAccess(const String & name1, const String & name2) const;
        AccessTypes getAccess(const String & name1, const Strings & names2) const;
        AccessTypes getAccess(const String & name1, const String & name2, const String & name3) const;
        AccessTypes getAccess(const String & name1, const String & name2, const Strings & names3) const;

        AccessTypes getParentAccess() const { return parent ? parent->access : 0; }
        AccessTypes getGrants() const { return grants; }
        AccessTypes getPartialRevokes() const { return getParentAccess() & ~access; }

        bool grant(AccessTypes add_access);
        bool grant(AccessTypes add_access, const String & name);
        bool grant(AccessTypes add_access, const Strings & names);
        bool grant(AccessTypes add_access, const String & name1, const String & name2);
        bool grant(AccessTypes add_access, const String & name1, const Strings & names2);
        bool grant(AccessTypes add_access, const String & name1, const String & name2, const String & name3);
        bool grant(AccessTypes add_access, const String & name1, const String & name2, const Strings & names3);

        bool revoke(AccessTypes remove_access, bool partial_revokes = false);
        bool revoke(AccessTypes remove_access, const String & name, bool partial_revokes = false);
        bool revoke(AccessTypes remove_access, const Strings & names, bool partial_revokes = false);
        bool revoke(AccessTypes remove_access, const String & name1, const String & name2, bool partial_revokes = false);
        bool revoke(AccessTypes remove_access, const String & name1, const Strings & names2, bool partial_revokes = false);
        bool revoke(AccessTypes remove_access, const String & name1, const String & name2, const String & name3, bool partial_revokes = false);
        bool revoke(AccessTypes remove_access, const String & name1, const String & name2, const Strings & names3, bool partial_revokes = false);

        void merge(const Node & other);

        bool operator ==(const Node & other) const;
        bool operator !=(const Node & other) const { return !(*this == other); }

    private:
        void addAccess(AccessTypes add_access);
        void addAccessRecalcGrants(AccessTypes add_access);
        void removeAccess(AccessTypes remove_access);

        template <typename ChildrenMapT = ChildrenMap>
        typename ChildrenMapT::iterator getIterator(const String & child_name);

        template <typename ChildrenMapT = ChildrenMap>
        void eraseOrIncrement(typename ChildrenMapT::iterator & it);

        AccessTypes access = 0;
        AccessTypes grants = 0;
        Node * parent = nullptr;
        std::unique_ptr<ChildrenMap> children;
    };

    Node root;
};

}
