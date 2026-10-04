#pragma once

#include <Core/Field.h>
#include <Core/UUID.h>
#include <Common/LoggingFormatStringHelpers.h>
#include <Common/SettingConstraintWritability.h>
#include <Common/SettingSource.h>
#include <Core/SettingsTierType.h>

#include <boost/container/flat_set.hpp>
#include <optional>
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
struct AlterSettingsProfileElements;
struct SettingsProfileElement;
class SettingsProfileElements;


/// Whether `allow_feature_tier` restricts anything at all. It usually does not, so this is checked before
/// looking up which tier a setting belongs to.
bool isAnyFeatureTierRestricted(const AccessControl & access_control);

/// The one place that decides `allow_feature_tier`, for every kind of setting and every kind of statement.
/// Returns the reason to refuse a change of `setting_name`, or nothing if its tier is allowed.
std::optional<PreformattedMessage> getFeatureTierRestriction(
    const AccessControl & access_control, std::string_view setting_name, SettingsTierType tier);


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
  *               <changeable_in_readonly/>
  *           </max_threads>
  *       </constraints>
  *   </user_profile>
  * </profiles>
  *
  * This class also checks that we are not in the read-only mode.
  * If a setting cannot be change due to the read-only mode this class throws an exception.
  * The value of `readonly` is understood as follows:
  * 0 - not read-only mode, no additional checks.
  * 1 - only read queries, as well as changing settings with <changeable_in_readonly/> flag.
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

    void set(const String & full_name, const Field & min_value, const Field & max_value, const std::vector<Field> & disallowed_values, SettingConstraintWritability writability);
    void get(const Settings & current_settings, std::string_view short_name, Field & min_value, Field & max_value, std::vector<Field> & disallowed_values, SettingConstraintWritability & writability) const;
    void get(const MergeTreeSettings & current_settings, std::string_view short_name, Field & min_value, Field & max_value, std::vector<Field> & disallowed_values, SettingConstraintWritability & writability) const;

    void merge(const SettingsConstraints & other);

    /// Checks whether `change` violates these constraints and throws an exception if so.
    void check(const Settings & current_settings, const SettingChange & change, SettingSource source) const;
    void check(const Settings & current_settings, const SettingsChanges & changes, SettingSource source) const;
    void check(const Settings & current_settings, SettingsChanges & changes, SettingSource source) const;
    /// `skip_config_defined_profiles` is a compatibility tolerance used only when applying a user's
    /// already-stored profiles at login: config-defined profiles in the chain are not re-validated.
    /// We cannot distinguish a legitimate user that references a config profile (e.g. `sql-console`)
    /// from one that was escalated via SQL before constraints were enforced, so we tolerate both
    /// rather than break the former. It is NOT a trust guarantee. Defaults to false (full enforcement).
    ///
    /// `actor_is_config_defined` is set when the identity managing settings/profiles (DDL, `SET profile`)
    /// is defined in the server config and is therefore a trusted admin: its own min/max/const/disallowed/
    /// writability constraints are not enforced against the change (it may create looser configurations).
    /// Structural rules still apply to everyone: the readonly mode, source restrictions (e.g.
    /// `max_sessions_for_user` is profile-only) and the server-wide `allow_feature_tier` policy.
    void check(const Settings & current_settings, const SettingsProfileElements & profile_elements, SettingSource source, bool skip_config_defined_profiles = false, bool actor_is_config_defined = false) const;

    void check(const Settings & current_settings, const AlterSettingsProfileElements & profile_elements, SettingSource source, bool actor_is_config_defined = false) const;

    /// Dropping a setting from a user, a role or a settings profile removes both its value and any constraint
    /// it declared, directly or through a profile it inherits. Refuses a write that removes a setting these
    /// constraints bind (CONST, or a min/max/disallowed bound), so that it cannot weaken them.
    void checkRemovedSettings(const SettingsProfileElements & old_elements, const SettingsProfileElements & new_elements) const;

    /// Checks whether resetting the specified settings to their defaults violates these constraints.
    void checkResetToDefault(const Settings & current_settings, const std::vector<String> & names, SettingSource source) const;

    /// Checks whether `change` violates these constraints and throws an exception if so. (setting short name is expected inside `changes`)
    void check(const MergeTreeSettings & current_settings, const SettingChange & change) const;
    void check(const MergeTreeSettings & current_settings, const SettingsChanges & changes) const;

    /// Checks whether `change` violates these and clamps the `change` if so.
    void clamp(const Settings & current_settings, SettingsChanges & changes, SettingSource source) const;

    /// `compatibility` gives a setting only a value that these constraints and `allow_feature_tier` accept,
    /// and leaves any other setting as it is. `restrictsCompatibility` tells whether any value can be refused.
    bool restrictsCompatibility() const;
    bool allowsValueFromCompatibility(std::string_view setting_name, const Field & value) const;

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
        std::vector<Field> disallowed_values{};

        bool operator ==(const Constraint & other) const;
        bool operator !=(const Constraint & other) const { return !(*this == other); }
    };

    struct Checker
    {
        Constraint constraint;
        using NameResolver = std::function<std::string_view(std::string_view)>;
        NameResolver setting_name_resolver;

        PreformattedMessage explain;
        int code = 0;

        PreformattedMessage tier_explain;
        int tier_code = 0;

        // Allows everything
        explicit Checker(NameResolver setting_name_resolver_)
            : setting_name_resolver(std::move(setting_name_resolver_))
        {}

        // Forbidden with explanation
        Checker(const PreformattedMessage & explain_, int code_)
            : constraint{.writability = SettingConstraintWritability::CONST}
            , explain(explain_)
            , code(code_)
        {}

        // Allow or forbid depending on range defined by constraint, also used to return stored constraint
        explicit Checker(const Constraint & constraint_, NameResolver setting_name_resolver_)
            : constraint(constraint_)
            , setting_name_resolver(std::move(setting_name_resolver_))
        {}

        // Perform checking. `actor_is_config_defined` skips the value constraints (CONST/min/max/disallowed)
        // for a trusted config-defined admin, while keeping the readonly mode, feature-tier and source restriction checks.
        bool check(SettingChange & change,
                   const Field & new_value,
                   ReactionOnViolation reaction,
                   SettingSource source,
                   bool actor_is_config_defined = false) const;
    };

    struct StringHash
    {
        using is_transparent = void;
        size_t operator()(std::string_view txt) const
        {
            return std::hash<std::string_view>{}(txt);
        }
    };

    /// Common logic for `check(Settings, SettingsChanges&)` and `clamp`. Both filter out unchanged settings
    /// (unless `compatibility` is present) and differ only in whether violations throw or get clamped to the nearest bound.
    void
    checkOrClamp(const Settings & current_settings, SettingsChanges & changes, ReactionOnViolation reaction, SettingSource source) const;

    bool checkImpl(
        const Settings & current_settings,
        SettingChange & change,
        ReactionOnViolation reaction,
        SettingSource source,
        bool ignore_unchanged_settings = false,
        bool actor_is_config_defined = false) const;

    void checkProfileElements(const Settings & current_settings,
                              const SettingsProfileElements & profile_elements,
                              SettingSource source,
                              boost::container::flat_set<UUID> & visited_profiles,
                              bool skip_config_defined_profiles,
                              bool actor_is_config_defined) const;

    void checkProfileElementValues(const Settings & current_settings,
                                   const SettingsProfileElement & element,
                                   SettingSource source,
                                   bool actor_is_config_defined) const;

    void checkProfileElementWritability(const SettingsProfileElement & element,
                                        const std::unordered_map<String, SettingConstraintWritability> & writability_map) const;

    /// True if the settings profile is defined in the server config (`users.xml`) rather than via SQL.
    bool isProfileConfigDefined(const UUID & profile_id) const;

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
