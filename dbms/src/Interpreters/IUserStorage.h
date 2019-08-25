#if 0
#pragma once

#include <Interpreters/IUsersManager.h>

#include <map>
#include <mutex>

namespace DB
{

class IUsersStorage
{
public:
    IUsersStorage() {}
    virtual ~IUsersStorage() = default;
    virtual std::vector<String> getAllNames() = 0;
    virtual std::unique_ptr<IUserSyncer> createStateSyncer() = 0;
};

class UserManager
{
    std::vector<UserPtr> findGrantedRolesTo(const std::vector<UserID> users);
};


class UsersMap
{
public:
    std::unique_lock<std::mutex> lock();
    void insert(UserPtr user);
    void remove(const UserID & user);
    UserPtr find(const String & name) const;
    UserPtr find(const UserID & user_id) const;

    void grantRole(UserID user, UserID role);
    void revokeRole(UserID user, UserID role);
    bool isRoleGranted(UserID user, UserID role);

    std::vector<UserPtr> findEnabledRolesFor(const UserID & user) const;
    std::vector<UserPtr> findGrantees(const User & user, bool recursive = false) const;

private:
    struct Node
    {
        UserPtr user;
    };
    std::mutex mutex;
    std::unordered_map<String, UserPtr> users_by_name;
    std::unordered_map<UserID, UserPtr> users_by_id;
};




    virtual UserPtr createUser(const User::State & initial_state)
    {
        if (!supportsName(name) || !supportsCreatingUsers())
            return nullptr;
        return std::make_shared<User>(initial_state, true, createSyncer());
    }

    virtual UserPtr getUserByName(const String & name)
    {
        if (!supportsName(name))
            return nullptr;
        auto user = std::make_shared<User>(State{name}, false, createSyncer());
        if (user->dropped())
            return nullptr;
        return user;
    }

    virtual UserPtr getUserByID(const UserID & user_id)
    {
        if (!supportsName(user_id.name))
            return nullptr;
        auto user = std::make_shared<User>(State{user_id}, false, createSyncer());
        if (user->dropped())
            return nullptr;
        return user;
    }

    virtual std::vector<UserID> getAllUsers() { return {}; }

protected:
    virtual bool supportsName(const String & name) { return true; }
    virtual std::unique_ptr<User::StateSyncer> createSyncer() = 0;
};



/** Default implementation of users manager used by native server application.
  * Manages fixed set of users listed in 'Users' configuration file.
  */
class UsersManager
{
public:
    class UserWithManagerData : public User
    {
        struct GrantedRole
        {
            UserWithManagerData * role;
            bool admin_option;
            bool enabled_by_default;
            bool enabled;
        };

        /// Roles granted to this role/user.
        std::unordered_map<UserID, GrantedRole> granted_roles;

        /// List of roles/users which this role is granted to.
        std::unordered_map<UserID, IUser *> granted_to;

        /// Aggregated set of privileges. Only active roles are taken in account.
        mutable std::shared_ptr<const Privileges> all_privileges;
        mutable std::shared_ptr<const Privileges> all_grant_option;
        mutable std::shared_ptr<const Privileges> enabled_privileges;
        mutable std::shared_ptr<const Privileges> enabled_grant_option;
    };


    UsersManager();

    void registerStorage(const std::shared_ptr<IUsersStorage> & storage)
    {
        auto storages = registered_storages;
        auto it = std::find(storages->begin(), storages->end(), storage);
        if (it != storages->end())
            return;
        auto new_storages = std::make_shared<std::vector<std::shared_ptr<IUsersStorage>>>(*storages);

    }

    UserPtr tryGetUser(const String & name)
    {
        {
            std::lock_guard lock{mutex};
            auto it = users_by_name.find(name);
            if (it != users_by_name.end())
                return it->second;
        }
        for (auto & storage : storages)
        {
            auto user = storage->getUserByName(name);
            if (user)
            {
                std::lock_guard lock{mutex};
                auto it = users_by_name.try_emplace(name, user).first;
                users_by_id.try_emplace(user->getID(), user);
                return it->second;
            }
        }
        return nullptr;
    }

    UserPtr tryGetUser(const UserID & user_id)
    {
        for (auto & storage : storages)
        {
            auto user = storage->getUserByID(user_id);
            if (user)
                return user;
        }
        return nullptr;
    }


    UserPtr getUser(const String & user_name) const
    {
        auto user = tryGetUser(user_name);
        if (user)
            return user;
        throw Exception (user_name + ": User not found");
    }

    UserPtr tryGetUser(const String & user_name) const
    {
        for (auto & storage : storages)
        {
            auto user = storage->getUser(user_name);
            if (user)
                return user;
        }
        return nullptr;
        throw Exception (user_name + ": User not found");
    }

    std::vector<UserPtr> getAllUsers() const
    {
        std::vector<UserPtr> result;
        for (auto & storage : storages)
        {
            auto users = storage->getAllUsers();
            result.insert(users.end(), std::make_move_iterator(users.begin()), std::make_move_iterator(users.end()));
        }
        return result;
    }

    /// Creates a user.
    void createUser(const String & user_name, bool if_not_exists)
    {
        bool created = storage_for_new_users->createUser(user_name);
        if (!created && !if_not_exists)
            throw Exception("User " + user_name + " already exists");
    }

    void dropUser(const String & user_name, bool if_exists)
    {
        bool dropped = false;
        for (auto & storage : storages)
            dropped |= storage->dropUser(user_name);
        if (!dropped && !if_exists)
            throw Exception("User " + user_name + " not found");
    }

protected:
    bool doesUserExist(const String & user_name)
    {

    }

    bool doesRoleExist(const String & role_name)
    {
        if (roles.count(role_name))
            return true;
        for (const auto & storage : storages)
            if (storage.getRole(role_name))
                return true;
        return false;
    }

private:
    std::vector<std::shared_ptr<IUsersStorage>> storages;

    std::mutex mutex;
    std::unordered_map<String, UserPtr> users_by_name;
    std::unordered_map<UserID, UserPtr> users_by_id;

    std::unordered_map<User *, User *>
};


/// Implementation of the old behaviour: users are read from users.xml, these users cannot get more privileges or be dropped.
class ConfigFileUsersStorage : public IUsersStorage
{
public:
    UserPtr getUser(const String & user_name) override;
    std::vector<UserPtr> getAllUsers() override;

    void syncUserCreated(UserPtr user, bool if_not_exists) override { throw Exception(LogicalError); }
    void syncUserDropped(UserPtr user, bool if_exists) override { throw Exception(LogicalError); }
    void syncRoleCreated(RolePtr role, bool if_not_exists) override { throw Exception(LogicalError); }
    void syncRoleDropped(RolePtr role, bool if_exists) override { throw Exception(LogicalError); }

    void syncPrivilegeGranted(RolePtr owner, PrivilegeType type, const String & database, const String & table, const Strings & columns) override { throw Exception(LogicalError); }
    void syncPrivilegeRevoked(RolePtr owner, PrivilegeType type, const String & database, const String & table, const Strings & columns) override { throw Exception(LogicalError); }
    void syncRoleGranted(RolePtr owner, const String & role) override { throw Exception(LogicalError); }
    void syncRoleRevoked(RolePtr owner, const String & role) override { throw Exception(LogicalError); }
};

}
#endif
