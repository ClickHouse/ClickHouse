#include <Access/SettingsConstraints.h>
#include <Access/SettingsProfileElement.h>
#include <Access/resolveSetting.h>
#include <Access/AccessControl.h>
#include <Core/Settings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/SettingSource.h>
#include <IO/WriteHelpers.h>

#include <bitset>
#include <string_view>
#include <unordered_map>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_ddl;
    extern const SettingsBool dynamic_disk_allow_from_env;
    extern const SettingsBool dynamic_disk_allow_include;
    extern const SettingsBool dynamic_disk_allow_from_zk;
    extern const SettingsUInt64 readonly;
}

namespace ErrorCodes
{
    extern const int READONLY;
    extern const int QUERY_IS_PROHIBITED;
    extern const int SETTING_CONSTRAINT_VIOLATION;
    extern const int UNKNOWN_SETTING;
}

namespace
{
struct SettingSourceRestrictions
{
    constexpr SettingSourceRestrictions() { allowed_sources.set(); }

    constexpr SettingSourceRestrictions(std::initializer_list<SettingSource> allowed_sources_)
    {
        for (auto allowed_source : allowed_sources_)
            setSourceAllowed(allowed_source, true);
    }

    constexpr bool isSourceAllowed(SettingSource source) { return allowed_sources[source]; }
    constexpr void setSourceAllowed(SettingSource source, bool allowed) { allowed_sources[source] = allowed; }

    std::bitset<SettingSource::COUNT> allowed_sources;
};

const std::unordered_map<std::string_view, SettingSourceRestrictions> SETTINGS_SOURCE_RESTRICTIONS = {
    {"max_sessions_for_user", {SettingSource::PROFILE}},
};

SettingSourceRestrictions getSettingSourceRestrictions(std::string_view name)
{
    auto settingConstraintIter = SETTINGS_SOURCE_RESTRICTIONS.find(name);
    if (settingConstraintIter != SETTINGS_SOURCE_RESTRICTIONS.end())
        return settingConstraintIter->second;
    return SettingSourceRestrictions(); // allows everything
}

/// Settings that are always allowed to change in readonly mode, regardless of the user profile's
/// `<constraints>` block. These are per-request HTTP routing, query-construction, and output
/// shaping settings (formerly special URL parameters like `?database=` and `?default_format=`)
/// that any client must be able to set on a GET request, even when `users.xml` does not declare
/// them as `<changeable_in_readonly/>`. Hard-coding the carve-out here (rather than shipping a
/// new `<constraints>` block in the default `users.xml`) keeps the new server compatible with
/// older `users.xml` files - and, importantly, lets older server versions continue to start up
/// against the new repo `programs/server/users.xml` (the integration-test framework mounts it
/// into backwards-compat containers, where unknown setting names in `<constraints>` would
/// otherwise be rejected with `UNKNOWN_SETTING`).
bool isAlwaysChangeableInReadonly(std::string_view name)
{
    /// HTTP routing / session.
    if (name == "database" || name == "default_format")
        return true;
    /// Output format selection and response compression.
    if (name == "format" || name == "input_format" || name == "output_format" || name == "compression")
        return true;
    /// Query-construction settings introduced for the HTTP "table as file" feature.
    if (name == "select" || name == "order" || name == "sort" || name == "filter")
        return true;
    /// Result-shaping (LIMIT / OFFSET / paging). The query itself remains read-only.
    if (name == "limit" || name == "offset" || name == "page")
        return true;
    /// FROM-less SELECT helper used by the HTTP "table as file" feature.
    if (name == "implicit_table_at_top_level")
        return true;
    return false;
}

}

SettingsConstraints::SettingsConstraints(const AccessControl & access_control_) : access_control(&access_control_)
{
}

SettingsConstraints::SettingsConstraints(const SettingsConstraints & src) = default;
SettingsConstraints & SettingsConstraints::operator=(const SettingsConstraints & src) = default;
SettingsConstraints::SettingsConstraints(SettingsConstraints && src) noexcept = default;
SettingsConstraints & SettingsConstraints::operator=(SettingsConstraints && src) noexcept = default;
SettingsConstraints::~SettingsConstraints() = default;


void SettingsConstraints::clear()
{
    constraints.clear();
    settings_alias_cache.clear();
}

void SettingsConstraints::set(const String & full_name, const Field & min_value, const Field & max_value, const std::vector<Field> & disallowed_values, SettingConstraintWritability writability)
{
    std::string resolved_name{resolveSettingName(full_name)};

    auto & constraint = constraints[resolved_name];

    if (full_name != resolved_name)
        settings_alias_cache[full_name] = resolved_name;

    if (!min_value.isNull())
        constraint.min_value = settingCastValueUtil(resolved_name, min_value);
    if (!max_value.isNull())
        constraint.max_value = settingCastValueUtil(resolved_name, max_value);
    if (!disallowed_values.empty())
    {
        for (const auto & allowed_value : disallowed_values)
            constraint.disallowed_values.emplace_back(settingCastValueUtil(resolved_name, allowed_value));
    }
    constraint.writability = writability;
}

void SettingsConstraints::get(const Settings & current_settings, std::string_view short_name, Field & min_value, Field & max_value, std::vector<Field> & disallowed_values, SettingConstraintWritability & writability) const
{
    // NOTE: for `Settings` short name is equal to full name
    auto checker = getChecker(current_settings, short_name);
    min_value = checker.constraint.min_value;
    max_value = checker.constraint.max_value;
    disallowed_values = checker.constraint.disallowed_values;
    writability = checker.constraint.writability;
}

void SettingsConstraints::get(const MergeTreeSettings &, std::string_view short_name, Field & min_value, Field & max_value, std::vector<Field> & disallowed_values, SettingConstraintWritability & writability) const
{
    auto checker = getMergeTreeChecker(short_name);
    min_value = checker.constraint.min_value;
    max_value = checker.constraint.max_value;
    disallowed_values = checker.constraint.disallowed_values;
    writability = checker.constraint.writability;
}

void SettingsConstraints::merge(const SettingsConstraints & other)
{
    if (access_control->doesSettingsConstraintsReplacePrevious())
    {
        for (const auto & [other_name, other_constraint] : other.constraints)
        {
            constraints[other_name] = other_constraint;
        }
    }
    else
    {
        for (const auto & [other_name, other_constraint] : other.constraints)
        {
            auto & constraint = constraints[other_name];
            if (!other_constraint.min_value.isNull())
                constraint.min_value = other_constraint.min_value;
            if (!other_constraint.max_value.isNull())
                constraint.max_value = other_constraint.max_value;
            if (!other_constraint.disallowed_values.empty())
                constraint.disallowed_values = other_constraint.disallowed_values;
            if (other_constraint.writability == SettingConstraintWritability::CONST)
                constraint.writability = SettingConstraintWritability::CONST; // NOTE: In this mode <readonly/> flag cannot be overridden to be false
        }
    }

    for (const auto & [other_alias, other_resolved_name] : other.settings_alias_cache)
        settings_alias_cache.try_emplace(other_alias, other_resolved_name);
}


void SettingsConstraints::check(const Settings & current_settings, const AlterSettingsProfileElements & profile_elements, SettingSource source) const
{
    check(current_settings, profile_elements.add_settings, source);
    check(current_settings, profile_elements.modify_settings, source);
    /// We don't check `drop_settings` here.
}

void SettingsConstraints::check(const Settings & current_settings, const SettingsProfileElements & profile_elements, SettingSource source) const
{
    for (const auto & element : profile_elements)
    {
        if (SettingsProfileElements::isAllowBackupSetting(element.setting_name))
            continue;

        if (element.value)
        {
            SettingChange value(element.setting_name, *element.value);
            check(current_settings, value, source);
        }

        if (element.min_value)
        {
            SettingChange value(element.setting_name, *element.min_value);
            check(current_settings, value, source);
        }

        if (element.max_value)
        {
            SettingChange value(element.setting_name, *element.max_value);
            check(current_settings, value, source);
        }

        /// Don't check disallowed_values here in the profile elements because they make constrains more restrictive
        /// and don't allow to bypass constraints from config by creating a user with custom constraints. The check
        /// for disallowed values are instead implemented in SettingsConstraints::Checker::check.

        SettingConstraintWritability new_value = SettingConstraintWritability::WRITABLE;
        SettingConstraintWritability old_value = SettingConstraintWritability::WRITABLE;

        if (element.writability)
            new_value = *element.writability;

        /// `set` stores a constraint under the canonical name of the setting, so look it up by that name.
        /// `Settings::resolveName` would not do: it does not know the `merge_tree_` prefix.
        auto setting_name = resolveSettingName(element.setting_name);
        auto it = constraints.find(setting_name);
        if (it != constraints.end())
            old_value = it->second.writability;

        if (new_value != old_value)
        {
            if (old_value == SettingConstraintWritability::CONST)
                throw Exception(ErrorCodes::SETTING_CONSTRAINT_VIOLATION, "Setting {} should not be changed", setting_name);
        }
    }
}

void SettingsConstraints::check(const Settings & current_settings, const SettingChange & change, SettingSource source) const
{
    checkImpl(current_settings, const_cast<SettingChange &>(change), THROW_ON_VIOLATION, source);
}

void SettingsConstraints::check(const Settings & current_settings, const SettingsChanges & changes, SettingSource source) const
{
    for (const auto & change : changes)
        check(current_settings, change, source);
}

void SettingsConstraints::check(const Settings & current_settings, SettingsChanges & changes, SettingSource source) const
{
    checkOrClamp(current_settings, changes, THROW_ON_VIOLATION, source);
}

void SettingsConstraints::checkResetToDefault(const Settings & current_settings, const std::vector<String> & names, SettingSource source) const
{
    /// A reset of a built-in setting is equivalent to assigning its declared default. The regular
    /// check also deliberately permits a reset that does not change the value.
    ///
    /// A `merge_tree_`-prefixed name is not a `Settings` setting, but it names a `MergeTreeSettings` one,
    /// which has a default too. Without this the reset was checked against the value the setting already
    /// had, which always passes. So a profile declaring `merge_tree_index_granularity MIN 16384` was
    /// escaped by `SET merge_tree_index_granularity = DEFAULT`, dropping it to the default of 8192.
    for (const auto & name : names)
    {
        if (settingIsBuiltin(name))
        {
            check(current_settings, SettingChange{name, settingDefaultValue(name)}, source);
            continue;
        }

        /// Custom settings have no declared default: resetting one removes it. There cannot be a
        /// value constraint for such a setting, but an existing value must still pass the readonly
        /// and source checks. Do not check an absent custom setting, preserving its no-op behavior.
        /// The value may be stored under any name of the setting, so look for all of them.
        Field current_value;
        bool has_current_value = current_settings.tryGet(name, current_value);
        for (const auto & equivalent_name : settingEquivalentNames(name))
        {
            if (has_current_value)
                break;
            has_current_value = current_settings.tryGet(equivalent_name, current_value);
        }
        if (has_current_value)
        {
            SettingChange change{name, current_value};
            getChecker(current_settings, Settings::resolveName(name)).check(change, current_value, THROW_ON_VIOLATION, source);
        }
    }
}

void SettingsConstraints::check(const MergeTreeSettings & current_settings, const SettingChange & change) const
{
    checkImpl(current_settings, const_cast<SettingChange &>(change), THROW_ON_VIOLATION);
}

void SettingsConstraints::check(const MergeTreeSettings & current_settings, const SettingsChanges & changes) const
{
    for (const auto & change : changes)
        check(current_settings, change);
}

void SettingsConstraints::clamp(const Settings & current_settings, SettingsChanges & changes, SettingSource source) const
{
    checkOrClamp(current_settings, changes, CLAMP_ON_VIOLATION, source);
}

void SettingsConstraints::checkOrClamp(const Settings & current_settings, SettingsChanges & changes, ReactionOnViolation reaction, SettingSource source) const
{
    /// If we filter out settings that match the current default here, `compatibility` will silently override them.
    /// So when `compatibility` is present, we keep unchanged settings so they are applied after `compatibility`.
    bool has_compatibility_setting = changes.tryGet("compatibility") != nullptr;
    std::erase_if(changes, [&](SettingChange & change)
    {
        return !checkImpl(current_settings, change, reaction, source, /*ignore_unchanged_settings=*/has_compatibility_setting);
    });
}

/// The value a setting holds now.
///
/// `Settings` keeps a `merge_tree_`-prefixed name as a custom setting, under the exact name that wrote it.
/// So a value written as `merge_tree_allow_experimental_block_number_column` is not found by looking up
/// `merge_tree_enable_block_number_column`, its other name for the same setting. Look under every name.
template <typename SettingsT>
bool getCurrentValueOfSetting(const SettingsT & current_settings, const String & name, Field & out_value)
{
    if (current_settings.tryGet(name, out_value))
        return true;

    if constexpr (std::is_same_v<SettingsT, Settings>)
    {
        for (const auto & equivalent_name : settingEquivalentNames(name))
        {
            if (current_settings.tryGet(equivalent_name, out_value))
                return true;
        }
    }
    return false;
}

/// The value in the type of the setting. `Settings::castValueUtil` returns a `merge_tree_`-prefixed value
/// unchanged, because it is a custom setting there, so resolve the real setting and use its type.
template <typename SettingsT>
Field castValueOfSetting(const String & name, const Field & value)
{
    if constexpr (std::is_same_v<SettingsT, Settings>)
        return settingCastValueUtil(name, value);
    else
        return SettingsT::castValueUtil(name, value);
}

/// Casts `change.value` to the setting's declared type and returns the result. Returns Null if we should skip the setting: either because
/// the value is unchanged (when `ignore_unchanged_settings` is false) or because the cast failed (when `throw_on_failure` is false).
template <typename SettingsT>
Field getNewValueToCheck(const SettingsT & current_settings, const SettingChange & change, bool ignore_unchanged_settings, bool throw_on_failure)
{
    Field current_value;
    bool has_current_value = getCurrentValueOfSetting(current_settings, change.name, current_value);

    if (!ignore_unchanged_settings && has_current_value && change.value == current_value)
        return {};

    /// Put the value into the type of the setting. This throws when the value does not fit, as
    /// `max_threads = 'abc'` does not. Callers that clamp a value instead of failing the query pass
    /// `throw_on_failure = false`, and for them the change is dropped instead.
    auto cast_value = [&](const Field & value, Field & out) -> bool
    {
        if (throw_on_failure)
        {
            out = castValueOfSetting<SettingsT>(change.name, value);
            return true;
        }
        try
        {
            out = castValueOfSetting<SettingsT>(change.name, value);
            return true;
        }
        catch (const Exception &)
        {
            return false;
        }
    };

    Field new_value;
    if (!cast_value(change.value, new_value))
        return {};

    if (!ignore_unchanged_settings && has_current_value)
    {
        /// Compare in the type of the setting. A profile with `merge_tree_enable_block_number_column = 1`
        /// stores a Bool, the same 1 in a query arrives as a UInt64, and comparing them as they are would
        /// make a change that keeps the value look like a real one.
        Field cast_current_value;
        if (cast_value(current_value, cast_current_value) && new_value == cast_current_value)
            return {};
    }

    return new_value;
}

bool SettingsConstraints::checkImpl(const Settings & current_settings,
                                    SettingChange & change,
                                    ReactionOnViolation reaction,
                                    SettingSource source,
                                    bool ignore_unchanged_settings) const
{
    std::string_view setting_name = Settings::resolveName(change.name);

    if (setting_name == "profile")
        return true;

    if (reaction == THROW_ON_VIOLATION)
    {
        try
        {
            access_control->checkSettingNameIsAllowed(setting_name);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            {
                if (const auto hints = current_settings.getHints(change.name); !hints.empty())
                {
                    e.addMessage(fmt::format("Maybe you meant {}", toString(hints)));
                }
            }
            throw;
        }
    }
    else if (!access_control->isSettingNameAllowed(setting_name))
        return false;

    Field new_value = getNewValueToCheck(current_settings, change, ignore_unchanged_settings, reaction == THROW_ON_VIOLATION);
    if (new_value.isNull())
        return false;

    if (ignore_unchanged_settings)
    {
        /// Compare in the type of the setting, as above. Casting cannot fail here: a value the setting
        /// cannot take is refused when it is written, both in the users configuration file and by
        /// `CREATE SETTINGS PROFILE`.
        Field current_value;
        if (getCurrentValueOfSetting(current_settings, change.name, current_value)
            && new_value == castValueOfSetting<Settings>(change.name, current_value))
            return true;
    }

    return getChecker(current_settings, setting_name).check(change, new_value, reaction, source);
}

bool SettingsConstraints::checkImpl(const MergeTreeSettings & current_settings, SettingChange & change, ReactionOnViolation reaction) const
{
    /// Resolve aliases upfront, mirroring the Settings overload above. Otherwise a user can
    /// bypass a constraint declared on the canonical setting name by writing to an alias,
    /// because the constraint lookup is a plain hashmap lookup on the (still un-resolved) name.
    std::string_view setting_name = MergeTreeSettings::resolveName(change.name);

    Field new_value = getNewValueToCheck(current_settings, change, /*ignore_unchanged_settings=*/false, reaction == THROW_ON_VIOLATION);
    if (new_value.isNull())
        return false;

    if (isAnyTierRestricted())
    {
        if (auto tier_checker = getTierChecker(setting_name, MergeTreeSettings::tryGetTierOfBuiltin(setting_name).value_or(SettingsTierType::PRODUCTION)))
            return tier_checker->check(change, new_value, reaction, SettingSource::QUERY);
    }

    return getMergeTreeChecker(setting_name).check(change, new_value, reaction, SettingSource::QUERY);
}

bool SettingsConstraints::Checker::check(SettingChange & change,
                                         const Field & new_value,
                                         ReactionOnViolation reaction,
                                         SettingSource source) const
{
    if (!explain.text.empty())
    {
        if (reaction == THROW_ON_VIOLATION)
            throw Exception(explain, code);
        return false;
    }

    std::string_view setting_name = setting_name_resolver(change.name);

    auto less_or_cannot_compare = [=](const Field & left, const Field & right)
    {
        if (reaction == THROW_ON_VIOLATION)
            return accurateLess(left, right);
        try
        {
            return accurateLess(left, right);
        }
        catch (const Exception &)
        {
            return true;
        }
    };

    auto equals_or_cannot_compare = [=](const Field & left, const Field & right)
    {
        if (reaction == THROW_ON_VIOLATION)
            return accurateEquals(left, right);
        try
        {
            return accurateEquals(left, right);
        }
        catch (const Exception &)
        {
            return true;
        }
    };


    if (constraint.writability == SettingConstraintWritability::CONST)
    {
        if (reaction == THROW_ON_VIOLATION)
            throw Exception(ErrorCodes::SETTING_CONSTRAINT_VIOLATION, "Setting {} should not be changed", setting_name);
        return false;
    }

    const auto & min_value = constraint.min_value;
    const auto & max_value = constraint.max_value;
    const auto & disallowed_values = constraint.disallowed_values;

    if (!min_value.isNull() && !max_value.isNull() && less_or_cannot_compare(max_value, min_value))
    {
        if (reaction == THROW_ON_VIOLATION)
            throw Exception(
                ErrorCodes::SETTING_CONSTRAINT_VIOLATION,
                "The maximum ({}) value is less than the minimum ({}) value for setting {}",
                max_value,
                min_value,
                setting_name);
        return false;
    }

    /// Track the effective value through clamping so that the disallowed-values loop below
    /// compares against the post-clamp value. Otherwise an overlap between a clamp target and
    /// a disallowed entry (e.g. min == disallowed) would let the clamped value through.
    Field effective_value = new_value;

    if (!min_value.isNull() && less_or_cannot_compare(effective_value, min_value))
    {
        if (reaction == THROW_ON_VIOLATION)
        {
            throw Exception(ErrorCodes::SETTING_CONSTRAINT_VIOLATION, "Setting {} shouldn't be less than {}",
                setting_name, applyVisitor(FieldVisitorToString(), min_value));
        }
        change.value = min_value;
        effective_value = min_value;
    }

    if (!max_value.isNull() && less_or_cannot_compare(max_value, effective_value))
    {
        if (reaction == THROW_ON_VIOLATION)
        {
            throw Exception(ErrorCodes::SETTING_CONSTRAINT_VIOLATION, "Setting {} shouldn't be greater than {}",
                setting_name, applyVisitor(FieldVisitorToString(), max_value));
        }
        change.value = max_value;
        effective_value = max_value;
    }

    for (const auto & value : disallowed_values)
    {
        bool equals = equals_or_cannot_compare(value, effective_value);
        if (equals)
        {
            if (reaction == THROW_ON_VIOLATION)
                throw Exception(ErrorCodes::SETTING_CONSTRAINT_VIOLATION, "Setting {} shouldn't be {}",
                    setting_name, applyVisitor(FieldVisitorToString(), value));
            /// On clamp paths there is no sensible value to clamp to — disallowed entries are a
            /// deny-list, not a range. Drop the change and let the caller proceed with the
            /// existing value rather than failing the query.
            return false;
        }
    }

    if (!getSettingSourceRestrictions(setting_name).isSourceAllowed(source))
    {
        if (reaction == THROW_ON_VIOLATION)
            throw Exception(ErrorCodes::READONLY, "Setting {} is not allowed to be set by {}", setting_name, toString(source));
        return false;
    }

    return true;
}

std::string_view SettingsConstraints::resolveSettingNameWithCache(std::string_view name) const
{
    if (auto it = settings_alias_cache.find(name); it != settings_alias_cache.end())
        return it->second;
    return name;
}

SettingsConstraints::Checker SettingsConstraints::getChecker(const Settings & current_settings, std::string_view setting_name) const
{
    /// The cache only knows the names constraints were declared with, which need not be the name a query
    /// uses. The caller has applied `Settings::resolveName` already, and that leaves a `merge_tree_`-prefixed
    /// name as it is, so resolve such a name here the way `set` resolved it before storing the constraint.
    String canonical_merge_tree_name;
    auto resolved_name = resolveSettingNameWithCache(setting_name);
    if (resolved_name.starts_with(MERGE_TREE_SETTINGS_PREFIX))
    {
        canonical_merge_tree_name = resolveSettingName(resolved_name);
        resolved_name = canonical_merge_tree_name;
    }
    if (!current_settings[Setting::allow_ddl] && resolved_name == "allow_ddl")
        return Checker(PreformattedMessage::create("Cannot modify 'allow_ddl' setting when DDL queries are prohibited for the user"),
                       ErrorCodes::QUERY_IS_PROHIBITED);

    /** The `readonly` value is understood as follows:
      * 0 - no read-only restrictions.
      * 1 - only read requests, as well as changing settings with `changeable_in_readonly` flag.
      * 2 - only read requests, as well as changing settings, except for the `readonly` setting.
      */

    if (current_settings[Setting::readonly] > 1 && resolved_name == "readonly")
        return Checker(PreformattedMessage::create("Cannot modify 'readonly' setting in readonly mode"), ErrorCodes::READONLY);

    /// Not `current_settings.getTier`: a `merge_tree_`-prefixed name is a `MergeTreeSettings` setting.
    if (isAnyTierRestricted())
    {
        if (auto tier_checker = getTierChecker(setting_name, settingGetTier(resolved_name)))
            return *tier_checker;
    }

    auto it = constraints.find(resolved_name);
    if (current_settings[Setting::readonly] == 1)
    {
        const bool changeable_in_readonly = (it != constraints.end()
                && it->second.writability == SettingConstraintWritability::CHANGEABLE_IN_READONLY)
            || isAlwaysChangeableInReadonly(resolved_name);
        if (!changeable_in_readonly)
            return Checker(PreformattedMessage::create("Cannot modify '{}' setting in readonly mode", setting_name),
                           ErrorCodes::READONLY);
    }
    if (it == constraints.end())
        return Checker(Settings::resolveName); // Allowed — no stored Constraint, do not dereference end().
    return Checker(it->second, Settings::resolveName);
}

/// Whether `allow_feature_tier` restricts anything at all. It usually does not, so this is checked before
/// looking up which tier a setting belongs to.
bool SettingsConstraints::isAnyTierRestricted() const
{
    return access_control
        && (!access_control->getAllowExperimentalTierSettings() || !access_control->getAllowPrivatePreviewTierSettings()
            || !access_control->getAllowBetaTierSettings());
}

/// The one place that enforces `allow_feature_tier`, for every kind of setting. Callers reach it only for
/// a setting a query really changes, so a value the server itself set is never refused.
std::optional<SettingsConstraints::Checker> SettingsConstraints::getTierChecker(std::string_view setting_name, SettingsTierType tier) const
{
    if (!access_control)
        return {};

    auto refuse = [&](std::string_view tier_name)
    {
        return Checker(
            PreformattedMessage::create(
                "Cannot modify setting '{}'. Changes to {} settings are disabled in the server config ('allow_feature_tier')",
                setting_name,
                tier_name),
            ErrorCodes::READONLY);
    };

    if (tier == SettingsTierType::EXPERIMENTAL && !access_control->getAllowExperimentalTierSettings())
        return refuse("EXPERIMENTAL");
    if (tier == SettingsTierType::PRIVATE_PREVIEW && !access_control->getAllowPrivatePreviewTierSettings())
        return refuse("PRIVATE PREVIEW");
    if (tier == SettingsTierType::BETA && !access_control->getAllowBetaTierSettings())
        return refuse("BETA");
    return {};
}

SettingsConstraints::Checker SettingsConstraints::getMergeTreeChecker(std::string_view short_name) const
{
    /// The canonical name, because `set` stored the constraint under that one.
    auto full_name = settingFullName<MergeTreeSettings>(MergeTreeSettings::resolveName(short_name));
    auto it = constraints.find(full_name);
    if (it == constraints.end())
        return Checker(MergeTreeSettings::resolveName); // Allowed
    return Checker(it->second, MergeTreeSettings::resolveName);
}

bool SettingsConstraints::Constraint::operator==(const Constraint & other) const
{
    return writability == other.writability && min_value == other.min_value && max_value == other.max_value && disallowed_values == other.disallowed_values;
}

bool operator ==(const SettingsConstraints & left, const SettingsConstraints & right)
{
    return left.constraints == right.constraints;
}
}
