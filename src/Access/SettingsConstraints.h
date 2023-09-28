#pragma once

#include <Access/SettingsProfileElement.h>
#include <Common/SettingsChanges.h>
#include <Common/SettingSource.h>
#include <unordered_map>

namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{
struct Settings;
struct MergeTreeSettings;
struct SettingChange;
class SettingsChanges;
class AccessControl;


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
  *               <const/>
  *           </force_index_by_date>
  *           <max_threads>
  *               <changable_in_readonly/>
  *           </max_threads>
  *       </constraints>
  *   </user_profile>
  * </profiles>
  *
  * This class also checks that we are not in the read-only mode.
  * If a setting cannot be change due to the read-only mode this class throws an exception.
  * The value of `readonly` is understood as follows:
  * 0 - not read-only mode, no additional checks.
  * 1 - only read queries, as well as changing settings with <changable_in_readonly/> flag.
  * 2 - only read queries and you can change the settings, except for the `readonly` setting.
  *
  */
class SettingsConstraints
{
public:
    explicit SettingsConstraints(const AccessControl & access_control_);
    SettingsConstraints(const SettingsConstraints & src);
    SettingsConstraints & operator=(const SettingsConstraints & src);
    SettingsConstraints(SettingsConstraints && src) noexcept;
    SettingsConstraints & operator=(SettingsConstraints && src) noexcept;
    ~SettingsConstraints();

    void clear();
    bool empty() const { return constraints.empty(); }

    void set(const String & full_name, const Field & min_value, const Field & max_value, SettingConstraintWritability writability);
    void get(const Settings & current_settings, std::string_view short_name, Field & min_value, Field & max_value, SettingConstraintWritability & writability) const;
    void get(const MergeTreeSettings & current_settings, std::string_view short_name, Field & min_value, Field & max_value, SettingConstraintWritability & writability) const;

    void merge(const SettingsConstraints & other);

    /// Checks whether `change` violates these constraints and throws an exception if so.
    void check(const Settings & current_settings, const SettingsProfileElements & profile_elements, SettingSource source) const;
    void check(const Settings & current_settings, const SettingChange & change, SettingSource source) const;
    void check(const Settings & current_settings, const SettingsChanges & changes, SettingSource source) const;
    void check(const Settings & current_settings, SettingsChanges & changes, SettingSource source) const;

    /// Checks whether `change` violates these constraints and throws an exception if so. (setting short name is expected inside `changes`)
    void check(const MergeTreeSettings & current_settings, const SettingChange & change) const;
    void check(const MergeTreeSettings & current_settings, const SettingsChanges & changes) const;

    /// Checks whether `change` violates these and clamps the `change` if so.
    void clamp(const Settings & current_settings, SettingsChanges & changes, SettingSource source) const;


    friend bool operator ==(const SettingsConstraints & left, const SettingsConstraints & right);
    friend bool operator !=(const SettingsConstraints & left, const SettingsConstraints & right) { return !(left == right); }

private:
    enum ReactionOnViolation
    {
        THROW_ON_VIOLATION,
        CLAMP_ON_VIOLATION,
    };

    struct Constraint
    {
        SettingConstraintWritability writability = SettingConstraintWritability::WRITABLE;
        Field min_value{};
        Field max_value{};

        bool operator ==(const Constraint & other) const;
        bool operator !=(const Constraint & other) const { return !(*this == other); }
    };

    struct Checker
    {
        Constraint constraint;
        using NameResolver = std::function<std::string_view(std::string_view)>;
        NameResolver setting_name_resolver;

        String explain;
        int code = 0;

        // Allows everything
        explicit Checker(NameResolver setting_name_resolver_)
            : setting_name_resolver(std::move(setting_name_resolver_))
        {}

        // Forbidden with explanation
        Checker(const String & explain_, int code_)
            : constraint{.writability = SettingConstraintWritability::CONST}
            , explain(explain_)
            , code(code_)
        {}

        // Allow or forbid depending on range defined by constraint, also used to return stored constraint
        explicit Checker(const Constraint & constraint_, NameResolver setting_name_resolver_)
            : constraint(constraint_)
            , setting_name_resolver(std::move(setting_name_resolver_))
        {}

        // Perform checking
        bool check(SettingChange & change,
                   const Field & new_value,
                   ReactionOnViolation reaction,
                   SettingSource source) const;
    };

    struct StringHash
    {
        using is_transparent = void;
        size_t operator()(std::string_view txt) const
        {
            return std::hash<std::string_view>{}(txt);
        }
    };

    bool checkImpl(const Settings & current_settings,
                  SettingChange & change,
                  ReactionOnViolation reaction,
                  SettingSource source) const;

    bool checkImpl(const MergeTreeSettings & current_settings, SettingChange & change, ReactionOnViolation reaction) const;

    Checker getChecker(const Settings & current_settings, std::string_view setting_name) const;
    Checker getMergeTreeChecker(std::string_view short_name) const;

    std::string_view resolveSettingNameWithCache(std::string_view name) const;

    // Special container for heterogeneous lookups: to avoid `String` construction during `find(std::string_view)`
    using Constraints = std::unordered_map<String, Constraint, StringHash, std::equal_to<>>;
    Constraints constraints;
    /// to avoid creating new string every time we cache the alias resolution
    /// we cannot use resolveName from BaseSettings::Traits because MergeTreeSettings have added prefix
    /// we store only resolved aliases inside the Constraints so to correctly search the container we always need to use resolved name
    std::unordered_map<std::string, std::string, StringHash, std::equal_to<>> settings_alias_cache;

    const AccessControl * access_control;
};

}
