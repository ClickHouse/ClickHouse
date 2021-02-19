#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Access/SettingsConstraints.h>
#include <Access/SettingsProfileElement.h>
#include <boost/container/flat_set.hpp>
#include <mutex>


namespace DB
{
/// Watches settings profiles for a specific user and roles.
class EnabledSettings
{
public:
    struct Params
    {
        UUID user_id;
        boost::container::flat_set<UUID> enabled_roles;
        SettingsProfileElements settings_from_enabled_roles;
        SettingsProfileElements settings_from_user;

        auto toTuple() const { return std::tie(user_id, enabled_roles, settings_from_enabled_roles, settings_from_user); }
        friend bool operator ==(const Params & lhs, const Params & rhs) { return lhs.toTuple() == rhs.toTuple(); }
        friend bool operator !=(const Params & lhs, const Params & rhs) { return !(lhs == rhs); }
        friend bool operator <(const Params & lhs, const Params & rhs) { return lhs.toTuple() < rhs.toTuple(); }
        friend bool operator >(const Params & lhs, const Params & rhs) { return rhs < lhs; }
        friend bool operator <=(const Params & lhs, const Params & rhs) { return !(rhs < lhs); }
        friend bool operator >=(const Params & lhs, const Params & rhs) { return !(lhs < rhs); }
    };

    ~EnabledSettings();

    /// Returns the default settings come from settings profiles defined for the user
    /// and the roles passed in the constructor.
    std::shared_ptr<const Settings> getSettings() const;

    /// Returns the constraints come from settings profiles defined for the user
    /// and the roles passed in the constructor.
    std::shared_ptr<const SettingsConstraints> getConstraints() const;

private:
    friend class SettingsProfilesCache;
    EnabledSettings(const Params & params_);

    void setSettingsAndConstraints(
        const std::shared_ptr<const Settings> & settings_, const std::shared_ptr<const SettingsConstraints> & constraints_);

    const Params params;
    SettingsProfileElements settings_from_enabled;
    std::shared_ptr<const Settings> settings;
    std::shared_ptr<const SettingsConstraints> constraints;
    mutable std::mutex mutex;
};
}
