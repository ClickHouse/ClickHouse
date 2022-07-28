#pragma once

#include <Common/SettingsChanges.h>
#include <unordered_map>


namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{
struct Settings;
struct SettingChange;
class SettingsChanges;
class AccessControlManager;


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
    SettingsConstraints(const AccessControlManager & manager_);
    SettingsConstraints(const SettingsConstraints & src);
    SettingsConstraints & operator =(const SettingsConstraints & src);
    SettingsConstraints(SettingsConstraints && src);
    SettingsConstraints & operator =(SettingsConstraints && src);
    ~SettingsConstraints();

    void clear();
    bool empty() const { return constraints.empty(); }

    void setMinValue(const std::string_view & setting_name, const Field & min_value);
    Field getMinValue(const std::string_view & setting_name) const;

    void setMaxValue(const std::string_view & setting_name, const Field & max_value);
    Field getMaxValue(const std::string_view & setting_name) const;

    void setReadOnly(const std::string_view & setting_name, bool read_only);
    bool isReadOnly(const std::string_view & setting_name) const;

    void set(const std::string_view & setting_name, const Field & min_value, const Field & max_value, bool read_only);
    void get(const std::string_view & setting_name, Field & min_value, Field & max_value, bool & read_only) const;

    void merge(const SettingsConstraints & other);

    /// Checks whether `change` violates these constraints and throws an exception if so.
    void check(const Settings & current_settings, const SettingChange & change) const;
    void check(const Settings & current_settings, const SettingsChanges & changes) const;
    void check(const Settings & current_settings, SettingsChanges & changes) const;

    /// Checks whether `change` violates these and clamps the `change` if so.
    void clamp(const Settings & current_settings, SettingsChanges & changes) const;

    friend bool operator ==(const SettingsConstraints & left, const SettingsConstraints & right);
    friend bool operator !=(const SettingsConstraints & left, const SettingsConstraints & right) { return !(left == right); }

private:
    struct Constraint
    {
        std::shared_ptr<const String> setting_name;
        bool read_only = false;
        Field min_value;
        Field max_value;

        bool operator ==(const Constraint & other) const;
        bool operator !=(const Constraint & other) const { return !(*this == other); }
    };

    enum ReactionOnViolation
    {
        THROW_ON_VIOLATION,
        CLAMP_ON_VIOLATION,
    };
    bool checkImpl(const Settings & current_settings, SettingChange & change, ReactionOnViolation reaction) const;

    Constraint & getConstraintRef(const std::string_view & setting_name);
    const Constraint * tryGetConstraint(const std::string_view & setting_name) const;

    std::unordered_map<std::string_view, Constraint> constraints;
    const AccessControlManager * manager = nullptr;
};

}
