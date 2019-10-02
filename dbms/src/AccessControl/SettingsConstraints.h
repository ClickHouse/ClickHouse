#pragma once

#include <Common/SettingsChanges.h>
#include <Core/Settings.h>

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}
}


namespace DB
{
struct Settings;

/** Checks if specified changes of settings are allowed or not.
  * If the changes are not allowed (i.e. violates some constraints) this class throws an exception.
  * The constraints are set by editing the `users.xml` file.
  *
  * For examples, the following lines in `users.xml` will set that `max_memory_usage` cannot be greater than 20000000000,
  * and `force_index_by_date` should be always equal to 0:
  *
  * <profiles>
  *   <user_profile>
  *       <max_memory_usage>10000000000</max_memory_usage>
  *       <force_index_by_date>0</force_index_by_date>
  *       ...
  *       <constraints>
  *           <max_memory_usage>
  *               <min>200000</min>
  *               <max>20000000000</max>
  *           </max_memory_usage>
  *           <force_index_by_date>
  *               <readonly/>
  *           </force_index_by_date>
  *       </constraints>
  *   </user_profile>
  * </profiles>
  *
  * This class also checks that we are not in the read-only mode.
  * If a setting cannot be change due to the read-only mode this class throws an exception.
  * The value of `readonly` value is understood as follows:
  * 0 - everything allowed.
  * 1 - only read queries can be made; you can not change the settings.
  * 2 - you can only do read queries and you can change the settings, except for the `readonly` setting.
  */
class SettingsConstraints
{
public:
    SettingsConstraints();
    SettingsConstraints(const SettingsConstraints & src);
    SettingsConstraints & operator =(const SettingsConstraints & src);
    SettingsConstraints(SettingsConstraints && src);
    SettingsConstraints & operator =(SettingsConstraints && src);
    ~SettingsConstraints();

    void clear();

    void setMinValue(const String & name, const Field & min_value);
    void setMaxValue(const String & name, const Field & max_value);
    void setReadOnly(const String & name, bool read_only);

    void check(const Settings & current_settings, const SettingChange & change) const;
    void check(const Settings & current_settings, const SettingsChanges & changes) const;

    /** Set multiple settings from "profile" (in server configuration file (users.xml), profiles contain groups of multiple settings).
      * The profile can also be set using the `set` functions, like the profile setting.
      */
    void setProfile(const String & profile_name, const Poco::Util::AbstractConfiguration & config);

    /// Loads the constraints from configuration file, at "path" prefix in configuration.
    void loadFromConfig(const String & path, const Poco::Util::AbstractConfiguration & config);

private:
    struct Constraint
    {
        bool read_only = false;
        Field min_value;
        Field max_value;
    };

    Constraint & getConstraintRef(size_t index);
    const Constraint * tryGetConstraint(size_t) const;

    std::unordered_map<size_t, Constraint> constraints_by_index;
};

}
