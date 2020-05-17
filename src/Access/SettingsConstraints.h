#pragma once

#include <Core/Field.h>
#include <Common/SettingsChanges.h>
#include <common/StringRef.h>
#include <unordered_map>


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
    bool empty() const { return constraints_by_index.empty(); }

    void setMinValue(const StringRef & setting_name, const Field & min_value);
    void setMinValue(size_t setting_index, const Field & min_value);
    Field getMinValue(const StringRef & setting_name) const;
    Field getMinValue(size_t setting_index) const;

    void setMaxValue(const StringRef & setting_name, const Field & max_value);
    void setMaxValue(size_t setting_index, const Field & max_value);
    Field getMaxValue(const StringRef & setting_name) const;
    Field getMaxValue(size_t setting_index) const;

    void setReadOnly(const StringRef & setting_name, bool read_only);
    void setReadOnly(size_t setting_index, bool read_only);
    bool isReadOnly(const StringRef & setting_name) const;
    bool isReadOnly(size_t setting_index) const;

    void set(const StringRef & setting_name, const Field & min_value, const Field & max_value, bool read_only);
    void set(size_t setting_index, const Field & min_value, const Field & max_value, bool read_only);
    void get(const StringRef & setting_name, Field & min_value, Field & max_value, bool & read_only) const;
    void get(size_t setting_index, Field & min_value, Field & max_value, bool & read_only) const;

    void merge(const SettingsConstraints & other);

    struct Info
    {
        StringRef name;
        Field min;
        Field max;
        bool read_only = false;
    };
    using Infos = std::vector<Info>;

    Infos getInfo() const;

    /// Checks whether `change` violates these constraints and throws an exception if so.
    void check(const Settings & current_settings, const SettingChange & change) const;
    void check(const Settings & current_settings, const SettingsChanges & changes) const;

    /// Checks whether `change` violates these and clamps the `change` if so.
    void clamp(const Settings & current_settings, SettingChange & change) const;
    void clamp(const Settings & current_settings, SettingsChanges & changes) const;

    /** Set multiple settings from "profile" (in server configuration file (users.xml), profiles contain groups of multiple settings).
      * The profile can also be set using the `set` functions, like the profile setting.
      */
    void setProfile(const String & profile_name, const Poco::Util::AbstractConfiguration & config);

    /// Loads the constraints from configuration file, at "path" prefix in configuration.
    void loadFromConfig(const String & path, const Poco::Util::AbstractConfiguration & config);

    friend bool operator ==(const SettingsConstraints & lhs, const SettingsConstraints & rhs);
    friend bool operator !=(const SettingsConstraints & lhs, const SettingsConstraints & rhs) { return !(lhs == rhs); }

private:
    struct Constraint
    {
        bool read_only = false;
        Field min_value;
        Field max_value;

        bool operator ==(const Constraint & rhs) const;
        bool operator !=(const Constraint & rhs) const { return !(*this == rhs); }
    };

    Constraint & getConstraintRef(size_t index);
    const Constraint * tryGetConstraint(size_t) const;

    std::unordered_map<size_t, Constraint> constraints_by_index;
};

}
