#pragma once

#include <Parsers/IAST_fwd.h>
#include <Core/Field.h>
#include <Core/UUID.h>
#include <Common/SettingsChanges.h>
#include <Common/SettingConstraintWritability.h>
#include <optional>
#include <unordered_map>
#include <vector>


namespace DB
{
struct Settings;
class SettingsConstraints;
struct AlterSettingsProfileElements;
class ASTSettingsProfileElement;
class ASTSettingsProfileElements;
class ASTAlterSettingsProfileElements;
class AccessControl;


struct SettingsProfileElement
{
    std::optional<UUID> parent_profile;

    String setting_name;
    std::optional<Field> value;
    std::optional<Field> min_value;
    std::optional<Field> max_value;
    std::vector<Field> disallowed_values;
    std::optional<SettingConstraintWritability> writability;

    auto toTuple() const { return std::tie(parent_profile, setting_name, value, min_value, max_value, disallowed_values, writability); }
    friend bool operator==(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return lhs.toTuple() == rhs.toTuple(); }
    friend bool operator!=(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return !(lhs == rhs); }
    friend bool operator <(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return lhs.toTuple() < rhs.toTuple(); }
    friend bool operator >(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return rhs < lhs; }
    friend bool operator <=(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return !(rhs < lhs); }
    friend bool operator >=(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return !(lhs < rhs); }

    SettingsProfileElement() = default;

    /// The constructor from AST requires the AccessControl if `ast.id_mode == false`.
    SettingsProfileElement(const ASTSettingsProfileElement & ast); /// NOLINT
    SettingsProfileElement(const ASTSettingsProfileElement & ast, const AccessControl & access_control);
    boost::intrusive_ptr<ASTSettingsProfileElement> toAST() const;
    boost::intrusive_ptr<ASTSettingsProfileElement> toASTWithNames(const AccessControl & access_control) const;

    bool empty() const { return !parent_profile && (setting_name.empty() || (!value && !min_value && !max_value && disallowed_values.empty() && !writability)); }

    bool isConstraint() const;

private:
    void init(const ASTSettingsProfileElement & ast, const AccessControl * access_control);
};


class SettingsProfileElements : public std::vector<SettingsProfileElement>
{
public:
    SettingsProfileElements() = default;

    /// The constructor from AST requires the AccessControl if `ast.id_mode == false`.
    SettingsProfileElements(const ASTSettingsProfileElements & ast, bool normalize_ = true); /// NOLINT
    SettingsProfileElements(const ASTSettingsProfileElements & ast, const AccessControl & access_control, bool normalize_ = true);

    boost::intrusive_ptr<ASTSettingsProfileElements> toAST() const;
    boost::intrusive_ptr<ASTSettingsProfileElements> toASTWithNames(const AccessControl & access_control) const;

    std::vector<UUID> findDependencies() const;
    bool hasDependencies(const std::unordered_set<UUID> & ids) const;
    void replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids);
    void copyDependenciesFrom(const SettingsProfileElements & src, const std::unordered_set<UUID> & ids);
    void removeDependencies(const std::unordered_set<UUID> & ids);

    void removeSettingsKeepProfiles();

    Settings toSettings() const;
    SettingsChanges toSettingsChanges() const;
    SettingsConstraints toSettingsConstraints(const AccessControl & access_control) const;
    UUIDs toProfileIDs() const;

    /// Normalizes this list of profile elements: removes duplicates and empty elements, and also sorts the elements
    /// in the following order: first profiles, then settings.
    /// The function is called automatically after parsing profile elements from an AST and
    /// at the end of an "ALTER PROFILE (USER/ROLE) command".
    void normalize();

    /// Appends all the elements of another list of profile elements to this list.
    void merge(const SettingsProfileElements & other, bool normalize_ = true);

    /// Applies changes from an "ALTER PROFILE (USER/ROLE)" command. Always normalizes the result.
    void applyChanges(const AlterSettingsProfileElements & changes);

    /// The settings whose effective value or constraints `applyChanges(changes)` would change. Effective
    /// means alias-resolved and with inherited profiles substituted, so how the change is written does not
    /// matter: an explicit value, an inherited profile, a DROP and an omission in a full replacement all
    /// show up the same. Only names: what the values may be is decided against the caller's own settings,
    /// by the checks over the elements the statement writes.
    Strings findChangedSettings(const AlterSettingsProfileElements & changes, const AccessControl & access_control) const;

    bool isBackupAllowed() const;
    static bool isAllowBackupSetting(const String & setting_name);
};

/// Everything the settings of these roles make effective for whoever holds them: their own settings and
/// those of the roles granted to them, recursively. A statement that makes a role effective, or stops
/// doing so, changes those settings without naming any of them.
SettingsProfileElements getSettingsOfRolesRecursively(const std::vector<UUID> & role_ids, const AccessControl & access_control);

struct AlterSettingsProfileElements
{
    bool drop_all_settings = false;
    bool drop_all_profiles = false;
    SettingsProfileElements add_settings;
    SettingsProfileElements modify_settings;
    SettingsProfileElements drop_settings;

    AlterSettingsProfileElements() = default;
    explicit AlterSettingsProfileElements(const SettingsProfileElements & ast);
    explicit AlterSettingsProfileElements(const ASTSettingsProfileElements & ast);
    explicit AlterSettingsProfileElements(const ASTAlterSettingsProfileElements & ast);
    AlterSettingsProfileElements(const ASTSettingsProfileElements & ast, const AccessControl & access_control);
    AlterSettingsProfileElements(const ASTAlterSettingsProfileElements & ast, const AccessControl & access_control);
};

}
