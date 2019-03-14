#pragma once

#include <memory>
#include <unordered_map>
#include <Core/Types.h>

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}
}

namespace DB
{
class Field;

/** Constraints for settings.
  */
class SettingsConstraints
{
public:
    SettingsConstraints();
    ~SettingsConstraints();

    /// Checks whether a new value is within the constraints for a setting.
    void check(const String & name, const String & value) const;
    void check(const String & name, const Field & value) const;

    template<typename SettingType>
    void check(const String & name, const SettingType & setting) const;

    /// Sets constraints for a setting.
    void setMaxValue(const String & name, const String & max_value);

    /** Set multiple settings from "profile" (in server configuration file (users.xml), profiles contain groups of multiple settings).
      * The profile can also be set using the `set` functions, like the profile setting.
      */
    void setProfile(const String & profile_name, const Poco::Util::AbstractConfiguration & config);

    /// Loads the constraints from configuration file, at "path" prefix in configuration.
    void loadFromConfig(const String & path, const Poco::Util::AbstractConfiguration & config);

private:
    class Constraint;
    template<typename SettingType> class ConstraintImpl;

    Constraint * findConstraint(const String & name);
    const Constraint * findConstraint(const String & name) const;
    Constraint & findOrCreateConstraint(const String & name);

    std::unordered_map<String, std::unique_ptr<Constraint>> name_to_constraint_map;
};

}
