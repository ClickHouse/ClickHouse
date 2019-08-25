#pragma once

#include <Core/Types.h>
#include <memory>
#include <unordered_map>


namespace DB
{

/// Represents a set of database/table/column privileges which a user or role has.
class Privileges
{
public:
    /// Access types.
    using Types = int; /// Combination of the constants below:
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

        /// User can create and drop users and roles.
        CREATE_USER = 0x40,

        /// All the privileges with GRANT_OPTION.
        ALL_PRIVILEGES = 0x7F,
    };

    static constexpr Types COLUMN_LEVEL = SELECT;
    static constexpr Types TABLE_LEVEL = COLUMN_LEVEL | INSERT | DELETE | ALTER | DROP;
    static constexpr Types DATABASE_LEVEL = TABLE_LEVEL | CREATE;
    static constexpr Types GLOBAL_LEVEL = DATABASE_LEVEL | CREATE_USER;

    /// Outputs a grant to string in readable format, for example "SELECT(column), INSERT ON mydatabase.*".
    static String accessToString(Types access);
    static String accessToString(Types access, const String & database);
    static String accessToString(Types access, const String & database, const String & table);
    static String accessToString(Types access, const String & database, const String & table, const String & column);
    static String accessToString(Types access, const String & database, const String & table, const Strings & columns);

    Privileges();
    ~Privileges();
    Privileges(const Privileges & src);
    Privileges & operator =(const Privileges & src);
    Privileges(Privileges && src);
    Privileges & operator =(Privileges && src);

    /// Returns true if these privileges don't grant anything.
    bool isEmpty() const;

    /// Removes all the privileges.
    void clear();

    /// Grants privileges.
    /// Returns false if the specified grant already exists.
    bool grant(Types access);
    bool grant(Types access, const String & database);
    bool grant(Types access, const String & database, const String & table);
    bool grant(Types access, const String & database, const String & table, const String & column);
    bool grant(Types access, const String & database, const String & table, const Strings & columns);

    /// Revokes privileges.
    /// Returns false if there is nothing to revoke.
    bool revoke(Types access);
    bool revoke(Types access, const String & database);
    bool revoke(Types access, const String & database, const String & table);
    bool revoke(Types access, const String & database, const String & table, const String & column);
    bool revoke(Types access, const String & database, const String & table, const Strings & columns);

    /// Returns granted privileges for a specified object.
    Types getAccess() const { return root.getAccess(); }
    Types getAccess(const String & database) const { return root.getAccess(database); }
    Types getAccess(const String & database, const String & table) const { return root.getAccess(database, table); }
    Types getAccess(const String & database, const String & table, const String & column) const { return root.getAccess(database, table, column); }
    Types getAccess(const String & database, const String & table, const Strings & columns) const { return root.getAccess(database, table, columns); }

    /// Checks if a specified privilege is granted. Returns false if it isn't.
    bool hasAccess(Types access) const { return !(access & ~getAccess()); }
    bool hasAccess(Types access, const String & database) const { return !(access & getAccess(database)); }
    bool hasAccess(Types access, const String & database, const String & table) const { return !(access & getAccess(database, table)); }
    bool hasAccess(Types access, const String & database, const String & table, const String & column) const { return !(access & getAccess(database, table, column)); }
    bool hasAccess(Types access, const String & database, const String & table, const Strings & columns) const { return !(access & getAccess(database, table, columns)); }

    /// Checks if a specified privilege is granted. Throws an exception if it isn't.
    void checkAccess(Types access) const;
    void checkAccess(Types access, const String & database) const;
    void checkAccess(Types access, const String & database, const String & table) const;
    void checkAccess(Types access, const String & database, const String & table, const String & column) const;
    void checkAccess(Types access, const String & database, const String & table, const Strings & columns) const;

    /// Merges two sets of privileges together.
    /// It's used to combine privileges when several roles are being used by the same user.
    Privileges & merge(const Privileges & other);

    struct Info
    {
        Types grants;
        Types partial_revokes;
        std::optional<String> database; /// If not set that means all databases.
        std::optional<String> table;    /// If not set that means all tables.
        std::optional<String> column;   /// If not set that means all columns.
    };

    /// Returns the information about the privileges.
    std::vector<Info> getInfo() const;

    friend bool operator ==(const Privileges & left, const Privileges & right);
    friend bool operator !=(const Privileges & left, const Privileges & right) { return !(left == right); }

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

        Types getAccess() const { return access; }
        Types getAccess(const String & name) const;
        Types getAccess(const Strings & names) const;
        Types getAccess(const String & name1, const String & name2) const;
        Types getAccess(const String & name1, const Strings & names2) const;
        Types getAccess(const String & name1, const String & name2, const String & name3) const;
        Types getAccess(const String & name1, const String & name2, const Strings & names3) const;

        Types getParentAccess() const { return parent ? parent->access : 0; }
        Types getGrants() const { return grants; }
        Types getPartialRevokes() const { return getParentAccess() & ~access; }

        bool grant(Types add_access);
        bool grant(Types add_access, const String & name);
        bool grant(Types add_access, const Strings & names);
        bool grant(Types add_access, const String & name1, const String & name2);
        bool grant(Types add_access, const String & name1, const Strings & names2);
        bool grant(Types add_access, const String & name1, const String & name2, const String & name3);
        bool grant(Types add_access, const String & name1, const String & name2, const Strings & names3);

        bool revoke(Types remove_access);
        bool revoke(Types remove_access, const String & name);
        bool revoke(Types remove_access, const Strings & names);
        bool revoke(Types remove_access, const String & name1, const String & name2);
        bool revoke(Types remove_access, const String & name1, const Strings & names2);
        bool revoke(Types remove_access, const String & name1, const String & name2, const String & name3);
        bool revoke(Types remove_access, const String & name1, const String & name2, const Strings & names3);

        void merge(const Node & other);

        bool operator ==(const Node & other) const;
        bool operator !=(const Node & other) const { return !(*this == other); }

    private:
        void addAccess(Types add_access);
        void addAccessRecalcGrants(Types add_access);
        void removeAccess(Types remove_access);

        template <typename ChildrenMapT = ChildrenMap>
        typename ChildrenMapT::iterator getIterator(const String & child_name);

        template <typename ChildrenMapT = ChildrenMap>
        void eraseOrIncrement(typename ChildrenMapT::iterator & it);

        Types access = 0;
        Types grants = 0;
        Node * parent = nullptr;
        std::unique_ptr<ChildrenMap> children;
    };

    Node root;
};

}
