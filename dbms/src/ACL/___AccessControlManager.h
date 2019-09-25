#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <ACL/IAccessControlElement.h>


namespace DB
{
class IAttributesStorage;


class AccessControlManager : public IAttributesStorageManager
{
public:
    using Storage = IAttributesStorage;
    using StoragePtr = std::shared_ptr<IAttributesStorage>;
    using ElementType = IAccessControlElement::Type;

    //std::pair<StoragePtr, UUID> findElement(const String & name, ElementType type);
    Storage * findStorage(UUID id) const;
    String findNameInCache(UUID id) const;

    //void checkNewName(UUID id, const String & new_name, ElementType type);
};

}


#if 0
#pragma once

#include <Interpreters/Privileges.h>

namespace DB
{

class UsersManager
{
public:
    UsersManager();
    ~UsersManager();

    std::vector<User> getAllUsers()
    {
        std::vector<User> users;
        for (auto & storage : storages)
        {
            auto ids = storage->findAll();
            for (const auto & id : ids)
                users.push_back({id, storage, this});
        }
        return users;
    }

    User getUser(const String & name)
    {

    }
    User getUser(const UserID & id);
    std::optional<User> tryGetUser(const String & name);
    std::optional<User> tryGetUser(const UserID & id);
    User createUser(const UserAttributes & initial_attrs, const String & storage_name);

private:
    std::vector<std::shared_ptr<IUserAttributesStorage>> storages;
    std::mutex mutex;
};


class User
{
public:
    User(const UserID & id_, const std::shared_ptr<IUserAttributesStorage> & storage_, UsersManager * manager_) : id(id_), storage(storage_), manager(manager_) {}
    User(const User & src) = default;
    User & operator =(const User & src) = default;

    const UserID & getID() const { return id; }
    const String & getName() const { return id.name; }
    UserAttributesPtr getAttributes() const { return storage->read(id); }

    void grant(AccessTypes access, bool with_grant_option = false)
    {
        storage->write(id, [&](UserAttributes & attrs)
        {
            attrs.privileges.grant(access);
            if (with_grant_option)
                attrs.grant_options.grant(access);
        });
    }

    void grant(AccessTypes access, const String & database)
    {
        storage->write(id, [&](UserAttributes & attrs)
        {
            attrs.privileges.grant(access, database);
            if (with_grant_option)
                attrs.grant_options.grant(access, database);
        });
    }

    void grantRole(const User & role, bool with_admin_option = false)
    {
        storage->write(id, [&](UserAttributes & attrs)
        {
            attrs.granted_roles[role.getID()].with_admin_option |= with_admin_option;
        });
    }

    void revokeRole(const User & role, bool only_admin_option = false)
    {
        storage->write(id, [&](UserAttributes & attrs)
        {
            auto it = attrs.granted_roles.find(role.getID());
            if (it == attrs.granted_roles.end())
                return;
            if (only_admin_option)
                it->second.with_admin_option = false;
            else
                attrs.granted_roles.erase(it);
        });
    }

    void setDefaultRoles(const std::vector<User> & roles)
    {
        storage->write(id, [&](UserAttributes & attrs)
        {
            for (const auto & role : roles)
                if (!attrs.granted_roles.count(role.getID()))
                    throw Exception("Role " + role.getName() + " is not granted, only granted roles can be set", ErrorCodes::ROLE_NOT_GRANTED);
            for (auto & granted_role_id_with_info : attrs.granted_roles)
            {
                auto & granted_role_info = granted_role_id_with_info.second;
                gr.enabled_by_default = false;
            }
            for (const auto & role : roles)
                attrs.granted_roles[role.getID()].enabled_by_default = true;
        });
    }

    std::vector<User> getDefaultRoles() const
    {
        auto attrs = getAttributes();
        if (!attrs)
        {
            throw NO_SUCH_USER;
            return;
        }
        std::vector<User> result;
        result.reserve(attrs.granted_roles.size());
        for (const auto & [id, gr] : attrs.granted_roles)
            if (gr.enabled_by_default)
                result.emplace_back(id);
        return result;
    }

    void drop()
    {
        storage->drop(id);
    }

private:
    const UserID id;
    std::shared_ptr<IUserAttributesStorage> storage;
    UsersManager * manager;
};


class UserContext
{
public:
    UserContext();
    UserContext(const User & user);
    UserContext(UserContext && src);
    UserContext & operator =(UserContext && src);

    User & getUser();
    const User & getUser() const;
    UserAttributesPtr getAttributes() const { return attributes; }

    void authorize(const String & password, const Poco::Net::IPAddress & address);

    void enableRoles(const std::vector<User> & roles);
    void enableDefaultRoles();
    std::shared_ptr<const Privileges> getEnabledPrivileges() const { return enabled_privileges; }
    std::shared_ptr<const Privileges> getEnabledGrantOptions() const { return enabled_grant_options; }

private:
    UserContext(const UserContext & src) = delete;
    UserContext & operator =(const UserContext & src) = delete;

    void onChanged(const UserID & user, const AttributesPtr & new_attrs)
    {
    }

    void update()
    {
    }

    User user;

    std::unordered_map<UserID, UserAttributesPtr> attrs;
    std::vector<std::unique_ptr<IUserAttributesStorage::Subscription>> subscriptions;

    /// Currently enabled roles. This is always a subset of granted roles.
    std::unordered_vector<UserID> enabled_roles;

    /// Privileges which this user has in total. This includes privileges from currently enabled roles.
    std::shared_ptr<const Privileges> enabled_privileges;
    std::shared_ptr<const Privileges> enabled_grant_options;

    mutable std::mutex mutex;
};


class ConfigFileUserAttributesStorage : public IUserAttributesStorage
{

};


class User;
using UserPtr = std::shared_ptr<User>;


class UserManager
{
public:
    UserManager()
    {
    }

    /*
    std::unique_ptr<IUserStateSyncer> createSyncer(const UserID & id) const
    {
        for (auto storage : storages)
        {
            std::unique_ptr<IUserStateSyncer> syncer;
            try
            {
                syncer = storage->createSyncer(id.name, false);
            }
            catch(Exception & e)
            {
                if (e.code() == NO_SUCH_USER)
                    continue;
                throw;
            }

            auto state = syncer.read();
            if (!state->dropped && (state->id == id))
                return syncer;
        }
        return nullptr;
    }

    std::unique_ptr<IUserStateSyncer> createSyncer(const String & name, bool create_new) const
    {
        for (auto storage : storages)
        {
            std::unique_ptr<IUserStateSyncer> syncer;
            try
            {
                syncer = storage->createSyncer(name, create_new);
            }
            catch(Exception & e)
            {
                if (e.code() == NO_SUCH_USER)
                    continue;
                throw;
            }

            if (!syncer.read()->dropped)
                return syncer;
        }
        return nullptr;
    }
    */

    UserPtr getUserByID(const UserID & id)
    {
        auto it = users_by_id.find(id);
        if (it != users_by_id.end())
            return it->second->shared_from_this();

        return std::make_shared<User>(this, id);
    }

    UserPtr getUserByName(const String & name)
    {
        auto it = users_by_name.find(id);
        if (it != users_by_name.end())
            return it->second->shared_from_this();

        return std::make_shared<User>(this, name, false);
    }

    UserPtr createUser(const UserState & new_state, const String & storage_name)
    {
        if (tryGetUserByName(new_state.id.name))
            throw USER_ALREADY_EXISTS;
        IUsersStorage * storage;
        for (auto & storage : storages)
            if (storage->getName() == storage_name)
            {
                storage->create(new_state);
                return std::make_shared<User>(this, new_state.id);
            }
        throw NO_SUCH_USERS_STORAGE;
    }

    std::vector<UserPtr> getAllUsers()
    {

    }



private:
    void registerUser(User * user)
    {
        manager->users_by_id.try_emplace(user->id, user);
        manager->users_by_name.try_emplace(user->id.name, user);
    }

    void unregisterUser(User * user)
    {

    }

    std::vector<std::unique_ptr<IUsersStorage>> storages;
    std::unordered_map<UserID, User *> users_by_id;
    std::unordered_map<String, User *> users_by_name;
    std::mutex mutex;
};



class User : std::enable_shared_from_this<User>
{
public:
    User(UserManager * manager_, const UserID & id_)
        : manager(manager_), id(id_)
    {
        manager->registerUser(this);
        //manager->users_by_id.try_emplace(id, this);
        //manager->users_by_name.try_emplace(id.name, this);
        readAttributes();
    }

    void readAttributes()
    {
        if (!storage)
            storage = manager->findStorage(id);
        if (!storage)
            throw Exception()
        std::tie(storage->read
    }

    void onChanged(const UserAttributes & attrs_)
    {
        attrs = attrs_;
        if (!attrs)
        {
            storage = nullptr;
            readAttributes();
            return;
        }


    }


    ~User()
    {
        manager->unregisterUser(this);
    }

    void drop()
    {
        if (storage)
            storage->drop(id);
    }

    UserStatePtr getState() const
    {
        while (true)
        {
            if (!syncer)
                syncer = manager->createSyncer(id);
            if (!syncer)
                return nullptr;
            auto state = syncer->read();
            if (!state->dropped && (state->id == id))
                return state;
            syncer.reset();
        }
    }


    const UserID id;
    UserManager * manager;

    UserAttributesPtr attributes;
    std::shared_ptr<IUserAttributesStorage> storage;
    std::unique_ptr<IUserAttributesStorage::Subscription> subscription;

    /// Currently enabled roles. This is always a subset of granted roles.
    std::unordered_set<UserID> enabled_roles;

    /// Sources of privileges for this user. This includes `enabled_roles`, then all roles granted to `enabled_roles`, and so on.
    /// This list doesn't include this user himself.
    std::unordered_set<UserPtr> source_roles;

    /// Privileges which this user has in total. This includes privileges from currently enabled roles.
    std::shared_ptr<const Privileges> enabled_privileges;
    std::shared_ptr<const Privileges> enabled_grant_options;

    mutable std::mutex mutex;
};



#endif
#if 0


    User(const State & initial_state, bool new_user, std::unique_ptr<StateSyncer> syncer,
         std::weak_ptr<UserManager> user_manager_);

        : syncer(std::move(syncer)), user_manager(user_manager_)
    {
        syncer->start(initial_state, new_user, &User::onChange);
    }

    ~User();

    StatePtr getState() const { return syncer->get(); }

    String getName() const { return getState()->name; }
    UserID getUserID() const { return getState()->user_id; }
    bool isRole() const { return getState()->is_role; }

    void enableRoles(const std::vector<UserID> & ids)
    {
        std::lock_guard lock{mutex};
        enabled_roles.clear();
        enabled_roles.insert(ids.begin(), ids.end());
        update();
    }

    void enableRolesByDefault()
    {
        StatePtr state = getState();
        if (!state->dropped)
        {
            std::vector<UserID> ids;
            ids.reserve(state->granted_roles.size());
            for (const auto & [granted_id, granted_info] : state->granted_roles)
                if (granted_info.enabled_by_default)
                    ids.emplace_back(granted_id);
            enableRoles(ids);
        }
    }

    std::shared_ptr<const Privileges> getEnabledPrivileges() const
    {
        std::lock_guard lock{mutex};
        return enabled_privileges;
    }

    std::shared_ptr<const Privileges> getEnabledGrantOptions() const
    {
        std::lock_guard lock{mutex};
        return enabled_grant_options;
    }

    void setEnabledByDefault(const std::vector<UserID> & enabled_by_default)
    {
        syncer->change([&](State & state)
        {
            for (const auto & id : enabled_by_default)
            {
                auto it = std::find(granted_role.begin(), granted_role.end(), id);
                if (it != granted_role.end())
                {
                    it->enabled_by_default =
                    gr
                    ids.emplace_back(granted_role.id);

            state.enabled_by_default = enabled_by_default;
        });
    }

    void drop()
    {
        syncer->change([&](State & state)
        {
            state.dropped = true;
        });
    }

private:
    void onChanged()
    {
        std::lock_guard lock{mutex};
        update();
    }

    void update()
    {

    }

        state = syncer.get();

        if s



        if (state->is_role)
        {
            for (const auto & user : find_uses_of_role(this))
                user.onChanged();
            return;
        }

        if (state->dropped)
        {
            std::lock_guard lock{mutex};
            enabled_roles.ids.clear();
            for (const auto & role : aggregation.all_enabled_roles)
                role->removeReference(this);
            aggregation.all_enabled_roles.clear();
            aggregation.enabled_privileges.reset();
            aggregation.enabled_grant_options.reset();
            std::lock_guard lock{references.mutex};
            for (const auto & ref : references.refs)
                ref->onChanged();
            references.refs.clear();
            return;
        }

        {
            std::lock_guard lock{mutex};

            /// Keep a role enabled only if it's still granted.
            for (auto it = enabled_roles.begin(); it != enabled_roles.end();)
            {
                if (!state->granted_roles.count(*it))
                    it = enabled_roles.erase(it);
                else
                    ++it;
            }

            sources_of_privileges.clear();
            size_t old_size = 0;
            for (const auto & id : enabled_roles)
            {
                auto role = get_role_function(id);
                if (role && std::find(sources_of_privileges)
                    sources_of_privileges.emplace_back(role);
            }

            while (sources_of_privileges.size() != old_size)
            {
                size_t cur_size = sources_of_privileges.size();
                for (size_t i = old_size; i != cur_size; ++i)
                {
                    auto role = sources_of_privileges[i];
                    for (const auto & granted_role : role->granted_roles)
                    {
                        auto role = get_role_function(granted_role.id);
                        if (role)
                            sources_of_privileges.emplace_back(role);
                    }
                }

            }

            enabled_privileges.clear();
            enabled_grant_options.clear();
            for (const auto & role : sources_of_privileges)
            {
                auto role_state = role->getState();
                if (!role_state->dropped())
                {
                    enabled_privileges.merge(role_state->privileges);
                    enabled_grant_options.merge(role_state->grant_options);
                }
            }
        }
    }

    UserID id;
    UserAttributesPtr attributes;
    std::shared_ptr<IUserAttributesStorage> storage;
    std::unique_ptr<IUserAttributesStorage::Subscription> subscription;

    /// Currently enabled roles. This is always a subset of granted roles.
    std::unordered_set<UserID> enabled_roles;

    /// Sources of privileges for this user. This includes `enabled_roles`, then all roles granted to `enabled_roles`, and so on.
    /// This list doesn't include this user himself.
    std::unordered_set<UserPtr> source_roles;

    /// Privileges which this user has in total. This includes privileges from currently enabled roles.
    std::shared_ptr<const Privileges> enabled_privileges;
    std::shared_ptr<const Privileges> enabled_grant_options;

    mutable std::mutex mutex;
};


class UserManager
{
public:
    UserPtr tryGetUser(const UserID & id);

private:

};






























        State(const String & user_name_, bool is_role, const std::function<void()> & on_change_);
        virtual ~State();

        String getName() const { std::lock_guard lock{mutex}; return user_id.name; }


        virtual void change(std::function<void(State &)> fn) = 0;



    protected:
        UserID user_id;
        bool is_role;

        /// Whether this user/role dropped.
        bool dropped = false;

        /// Set of privileges granted to this role/user.
        /// This set doesn't include the privileges from granted roles.
        Privileges privileges;
        Privileges grant_options;

        struct GrantedRole
        {
            UserID role;
            bool admin_option;
            bool enabled_by_default;
            bool enabled;
        };

        /// Roles granted to this role/user.
        std::vector<GrantedRole> granted_roles;

        std::mutex mutex;
    };

    User(std::unique_ptr<State> state);
    virtual ~IUser();

    String getName() const { state->getName(); }
    const UserID & getUserID() const { return user_id; }
    bool isRole() const { return is_role; }

    void drop()
    {
        state->change([&](State & state)
        {
            state.dropped = true;
        });
    }

    void grant(AccessTypes access, const std::optional<String> & database, const std::optional<String> & table, const std::optional<String> & column, bool with_grant_option)
    {
        if (apply(GrantChange{*this, access, database, table, columns, with_grant_option}))
            onChange();
    }

    void revoke(AccessTypes access, const std::optional<String> & database, const std::optional<String> & table, const std::optional<String> & column, bool only_grant_option)
    {
        if (apply(RevokeChange{*this, access, database, table, columns, only_grant_option}))
            onChange();
    }



        std::unique_lock lock{mutex};
        addDependency(grantee->getName());

        grantee->changeAndSync([&]
        {
            auto grantor = grantee->grantors[getName()];
            grantor.privileges.merge(access, database, table, column);
            if (with_grant_option)
                grantor.grant_options.merge(access, database, table, column);
        });
        while (true)
        {
            auto grantor = grantee->grantors[getName()];
            grantor.privileges.makeUnion(access, database, table, column);
            if (with_grant_option)
                grantor.grant_options.makeUnion(access, database, table, column);
            if (grantee->save())
                return;
            grantee->load();
        }
    }

    void revoke(std::shared_ptr<IUser> grantee, AccessTypes access, const std::optional<String> & database, const std::optional<String> & table, const std::optional<String> & column, bool only_grant_option);

    void grantRole(const std::shared_ptr<IUser> & role, bool with_admin_option)
    {
        if (changeAndSync(&IUser::grantRoleImpl, role, with_admin_option))
        {
            std::lock_guard lock{role->mutex};
            role->dependents[getUserID()] = this;
        }
    }

    void revokeRole(std::shared_ptr<IUser> grantee, std::shared_ptr<IUser> role, bool only_admin_option);

    std::shared_ptr<const Privileges> getEnabledPrivileges() const;
    std::shared_ptr<const Privileges> getEnabledGrantOption() const;

protected:
    virtual bool apply(Change & change) = 0;

private:
    class Change
    {
    public:
        Change() {}
        virtual ~Change() {}
        virtual bool commit() = 0;
        virtual void revert() = 0;
    };

    class GrantChange : public Change
    {
    public:
        GrantChange(IUser & user, AccessTypes access, const std::optional<String> & database, const std::optional<String> & table, const std::optional<String> & column, bool with_grant_option)
        {

        }

        bool commit() override
        {

        }

        void revert() override
        {

        }

    private:
        IUser & user;
        AccessTypes access;
        const std::optional<String> & database;
        const std::optional<String> & table;
        const std::optional<Strings> & columns;
        bool with_grant_option;
    };

    class DropChange : public Change
    {
    public:
        DropChange(IUser & user_) : user(user_) {}
        bool commit() override
        {
            assert(!user.dropped);
            user.dropped = true;
        }

        void revert() override
        {
            user.dropped = false;
        }

    private:
        IUser & user;
    };

    virtual void sync(Change & change)
    {
        std::lock_guard lock{mutex};
        while (true)
        {
            if (!change.apply())
                return;
            bool version_mismatch = false;
            try
            {
                if (dropped)
                    delete(version_mismatch);
                else
                    save(version_mismatch);
            }
            catch (...)
            {
                change.revert();
                throw();
            }
            if (!version_mismatch)
            {
                onChange();
                return true;
            }
            change.revert();
            load();
        }
    }

    enum class SaveResult { SAVED_OK, VERSION_MISMATCH };
    virtual void save() = 0;
    virtual void load() = 0;


    void dropImpl()
    {
        dropped = true;
    }

    bool grantImpl(AccessTypes access, const std::optional<String> & database, const std::optional<String> & table, const std::optional<String> & column, bool with_grant_option)
    {
        return privileges.merge(access, database, table, columns);
    }

    bool grantRoleImpl(const std::shared_ptr<IUser> & role, bool with_admin_option)
    {
        auto [it, inserted] = granted_roles.try_emplace(role->getUserID(), {});
        auto & granted_role = it->second;
        if (inserted)
        {
            granted_role.role = role;
            granted_role.admin_option = with_admin_option;
            granted_role.enabled_by_default = false;
            granted_role.enabled = false;
            return true;
        }

        if (granted_role.admin_option || !with_admin_option)
            return false;
        it->second.admin_option |= with_admin_option;
        return true;
    }

    bool addDependency(const String & dependency)
    {
        return changeAndSync([&]
        {
            checkNotDropped();
            if (std::find(dependencies.begin(), dependencies.end(), dependency) != dependencies.end())
                return false;
            dependencies.emplace_back(grantee->getName());
            return true;
        });
    }

    template <typename Func>
    bool changeAndSync(const Func & func, IUser & user)
    {
        while (true)
        {
            user.checkNotDropped();
            if (!func())
                return false;
            if (user.save())
            {, const UpdateDependenciesFunction & update_dependencies
                user.update_dependencies_function(user.dependencies);
                return true;
            }
            user.load();
        }
    }

    void onChange()
    {

        for (auto & dependent : dependents)
            dependent->resetAggregatedPrivileges();
    }

    void resetAggregatedPrivileges()
    {
        std::lock_guard lock{mutex};
        all_privileges.reset();
        all_grant_option.reset();
        enabled_privileges.reset();
        enabled_grant_option.reset();
    }

    const UserID user_id;
    const bool is_role;

    /// Whether this user/role dropped.
    bool dropped = false;

    /// Set of privileges granted to this role/user.
    /// This set doesn't include the privileges from granted roles.
    Privileges privileges;
    Privileges grant_options;

    struct GrantedRole
    {
        std::shared_ptr<IUser> role;
        bool admin_option;
        bool enabled_by_default;
        bool enabled;
    };

    /// Roles granted to this role/user.
    std::unordered_map<UserID, GrantedRole> granted_roles;

    /// List of roles/users which this role is granted to.
    std::unordered_map<UserID, IUser *> dependents;

    /// Aggregated set of privileges. Only active roles are taken in account.
    mutable std::shared_ptr<const Privileges> all_privileges;
    mutable std::shared_ptr<const Privileges> all_grant_option;
    mutable std::shared_ptr<const Privileges> enabled_privileges;
    mutable std::shared_ptr<const Privileges> enabled_grant_option;

    mutable std::mutex mutex;
};


class ZookeeperUser : public IUser
{
public:
    ZookeeperUser(bool new_user)
    {
        if (new_user)
            createNode();
        else
            getNode();
    }

protected:
    bool apply(Change & change) override;

private:
    int version;
};


class IUsersStorage
{
public:
    using AccessTypes = Privileges::AccessTypes;
    IUsersStorage() {}
    virtual ~IUsersStorage() = default;

    /// Creates a user if there is no user with such name.
    virtual std::pair<UserPtr, bool /* created */> createUser(const String & user_name);

    /// Get a user by name. Returns nullptr if there is no such user.
    virtual UserPtr tryGetUser(const String & user_name) = 0;

    /// Returns all the users stored in this storage.
    virtual std::vector<UserPtr> getAllUsers() = 0;

    virtual void load(IUser & user) = 0;
    virtual bool save(const IUser & user) = 0;
};

}


#include <Core/Types.h>
#include <Interpreters/Privileges.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace Poco
{
    namespace Net
    {
        class IPAddress;
    }

    namespace Util
    {
        class AbstractConfiguration;
    }
}


namespace DB
{


/// Allow to check that address matches a pattern.
class IAddressPattern
{
public:
    virtual bool contains(const Poco::Net::IPAddress & addr) const = 0;
    virtual ~IAddressPattern() {}
};


class AddressPatterns
{
private:
    using Container = std::vector<std::shared_ptr<IAddressPattern>>;
    Container patterns;

public:
    bool contains(const Poco::Net::IPAddress & addr) const;
    void addFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);
};


class Role
{
public:
    Role(UsersManager * manager_, const String & name_) : manager(manager_), name(name_) {}
    virtual ~Role() {};

    /// Returns the role's name.
    const String & getName() const { return name; }

    /// Returns true if this role is actually a user.
    virtual bool isUser() const { return false; }

    static const String ALL_DATABASES;
    static const String ALL_TABLES;
    static const Strings ALL_COLUMNS;

    /// Grants privileges. Doesn't do anything if they are already granted.
    void grant(Type type, const String & database = ALL_DATABASES, const String & table = ALL_TABLES, const Strings & columns = ALL_COLUMNS)
    {
        std::lock_guard guard{mutex};
        privileges.addGrant(type, database, table, columns);
        roleChanged();
    }

    /// Revokes privileges. Throws an exception if they aren't granted.
    void revoke(Type type, const String & database = ALL_DATABASES, const String & table = ALL_TABLES, const Strings & columns = ALL_COLUMNS)
    {
        std::lock_guard guard{mutex};
        privileges.removeGrant(type, database, table, columns);
        roleChanged();
    }

    /// Grants all the privileges.
    void grantAll(const String & database = ALL_DATABASES, const String & table = ALL_TABLES, const Strings & columns = ALL_COLUMNS)
    {
        std::lock_guard guard{mutex};
        privileges.addAllGrants(database, table, columns);
        roleChanged();
    }

    /// Grants all the privileges except GRANT_OPTION.
    void grantAllExceptGrantOption(const String & database = ALL_DATABASES, const String & table = ALL_TABLES, const Strings & columns = ALL_COLUMNS)
    {
        std::lock_guard guard{mutex};
        privileges.grantAllExceptGrantOption(database, table, columns);
        roleChanged();
    }

    /// Revokes all granted privileges.
    void revokeAll(const String & database = ALL_DATABASES, const String & table = ALL_TABLES, const Strings & columns = ALL_COLUMNS)
    {
        std::lock_guard guard{mutex};
        privileges.revokeAll(database, table, columns);
        roleChanged();
    }

    /// Returns a list of all the granted privileges.
    std::vector<Privileges::Grant> getGrants() const
    {
        std::lock_guard guard{mutex};
        return privileges.getGrants();
    }

    void grantRole(const String & role)
    {
        std::lock_guard guard{mutex};
        granted_roles.emplace_back(role);
    }

    void revokeRole(const String & role);
    void revokeRoleIfGranted(const String & role);
    Strings getGrantedRoles() const { return granted_roles; }

    virtual std::shared_ptr<const Privileges> getEnabledPrivileges() const
    {
        if (!enabled_privileges)
        {
            auto result = std::make_unique<Privileges>(privileges);
            for (const String & role : granted_roles)
                enabled_privileges.merge(*manager->getRole(role)->getEnabledPrivileges());
            enabled_privileges = std::move(result);
        }
        return enabled_privileges;
    }

    /// Makes an AST Tree showing grants (for the command "SHOW GRANTS FOR").
    virtual ASTPtr getGrantsTree() const;

protected:
    void roleChanged()
    {
        setNeedUpdateEnabledPrivileges();
        manager->saveGrants(name);
    }

    virtual void setNeedUpdateEnabledPrivileges()
    {
        enabled_privileges = nullptr;
        for (auto & dependant_role : manager->getDependantRoles(name))
            dependant_role->setNeedUpdateEnabledPrivileges();
    }

    struct GrantedRole { RolePtr role; bool admin_option; bool default; bool enabled; }

    struct Granted
    {
        Privileges privileges;
        GrantedRole role;
    };

    std::unordered_map<String, Granted> granted;
    std::shared_ptr<const Privileges> enabled_privileges;


    UsersManager * manager;
    String name;
    Privileges privileges;
    Strings granted_roles;
    std::shared_ptr<const Privileges> enabled_privileges;
    std::mutex mutex;
};

using RolePtr = using std::shared_ptr<Role>;


/** User and ACL.
  */
class User
{
public:
    User(const String & name_);
    ~User() override;

    const String & getName() const { return name; }
    bool isRole() const { return is_role; }
    bool isDropped() const { return is_dropped; }

    /// Checks the password and address, throws an exception if access denied.
    virtual void login(const String & password, const Poco::Net::IPAddress & address) const {}

    Privileges getGrantedPrivileges() const
    {
        std::lock_guard lock{mutex};
        return granted_privileges;
    }

    std::vector<std::shared_ptr<User>> getGrantedRoles() const
    {
        std::lock_guard lock{mutex};
        return granted_roles;
    }

    void setEnabledRoles(const std::vector<String> & role_names)
    {
        std::lock_guard lock{mutex};
        return enabled_roles;
    }

    std::vector<std::shared_ptr<User>> getEnabledRoles() const
    {
        std::lock_guard lock{mutex};
        return enabled_roles;
    }

    std::shared_ptr<const Privileges> getEnabledPrivileges() const
    {
        std::lock_guard lock{mutex};
        if (!enabled_privileges)
        {
            auto result = std::make_shared<Privileges>();

            enabled_privileges = std::make_shared<Privileges>(calculateEnabledPrivileges());
        return enabled_privileges;
    }

protected:
    void markAsDropped()
    {
        is_dropped = true;
        privileges.revoke(ALL_PRIVILEGES_WITH_GRANT_OPTION);
        granted_roles.clear();
        enabled_privileges.reset();
    }

    virtual Privileges calculateEnabledPrivileges() const = 0;

    String name;
    bool can_login = false;
    bool is_dropped = false;
    Privileges granted_privileges;
    std::vector<std::shared_ptr<User>> granted_roles;
    mutable std::shared_ptr<const Privileges> enabled_privileges;
    mutable std::mutex mutex;

#if 0
    void setDefaultRolesForUser(const Strings & default_roles);
    void setEnabledRolesForUser(const Strings & roles);
    void setEnabledRolesForUserByDefault(const String & user_name);
    std::shared_ptr<const Privileges> getEnabledPrivileges() const;

    /// Required password. Could be stored in plaintext or in SHA256.
    String password;
    String password_sha256_hex;

    String profile;
    String quota;

    AddressPatterns addresses;

    /// List of allowed databases.
    using DatabaseSet = std::unordered_set<std::string>;
    DatabaseSet databases;

    /// Table properties.
    using PropertyMap = std::unordered_map<std::string /* name */, std::string /* value */>;
    using TableMap = std::unordered_map<std::string /* table */, PropertyMap /* properties */>;
    using DatabaseMap = std::unordered_map<std::string /* database */, TableMap /* tables */>;
    DatabaseMap table_props;

    User(const String & name_, const String & config_elem, const Poco::Util::AbstractConfiguration & config);

    /// Privileges.
    Strings default_roles;
    Strings enabled_roles;
    std::shared_ptr<const Privileges> enabled_privileges;
#endif
};

using UserPtr = std::shared_ptr<User>;


class UserFromConfigFile : public User
{
public:
    UserFromConfigFile(UsersManager *manager_, const String & name_, const String & config_elem, const Poco::Util::AbstractConfiguration & config);
};


}
#endif
