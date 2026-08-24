#pragma once

#include <Core/Field.h>
#include <Core/UUID.h>
#include <Common/SettingConstraintWritability.h>
#include <optional>
#include <unordered_map>
#include <vector>


namespace DB
{
struct Settings;
class SettingsChanges;
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
    std::shared_ptr<ASTSettingsProfileElement> toAST() const;
    std::shared_ptr<ASTSettingsProfileElement> toASTWithNames(const AccessControl & access_control) const;

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

    std::shared_ptr<ASTSettingsProfileElements> toAST() const;
    std::shared_ptr<ASTSettingsProfileElements> toASTWithNames(const AccessControl & access_control) const;

    std::vector<UUID> findDependencies() const;
    bool hasDependencies(const std::unordered_set<UUID> & ids) const;
    void replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids);
    void copyDependenciesFrom(const SettingsProfileElements & src, const std::unordered_set<UUID> & ids);
    void removeDependencies(const std::unordered_set<UUID> & ids);

    void removeSettingsKeepProfiles();

    Settings toSettings() const;
    SettingsChanges toSettingsChanges() const;
    SettingsConstraints toSettingsConstraints(const AccessControl & access_control) const;
    std::vector<UUID> toProfileIDs() const;

    /// Normalizes this list of profile elements: removes duplicates and empty elements, and also sorts the elements
    /// in the following order: first profiles, then settings.
    /// The function is called automatically after parsing profile elements from an AST and
    /// at the end of an "ALTER PROFILE (USER/ROLE) command".
    void normalize();

    /// Appends all the elements of another list of profile elements to this list.
    void merge(const SettingsProfileElements & other, bool normalize_ = true);

    /// Applies changes from an "ALTER PROFILE (USER/ROLE)" command. Always normalizes the result.
    void applyChanges(const AlterSettingsProfileElements & changes);

    bool isBackupAllowed() const;
    static bool isAllowBackupSetting(const String & setting_name);
};

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
