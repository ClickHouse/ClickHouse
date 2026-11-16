#pragma once

#include <Access/SettingsProfileElement.h>
#include <base/defines.h>
#include <Core/UUID.h>
#include <boost/container/flat_set.hpp>
#include <mutex>


namespace DB
{
struct SettingsProfilesInfo;

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

    /// Returns the default settings come from settings profiles defined for the user
    /// and the roles passed in the constructor.
    std::shared_ptr<const SettingsProfilesInfo> getInfo() const;

    ~EnabledSettings();

private:
    friend class SettingsProfilesCache;
    explicit EnabledSettings(const Params & params_);
    void setInfo(const std::shared_ptr<const SettingsProfilesInfo> & info_);

    const Params params;
    std::shared_ptr<const SettingsProfilesInfo> info TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;
};
}
