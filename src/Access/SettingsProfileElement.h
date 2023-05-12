#pragma once

#include <Core/Field.h>
#include <Core/UUID.h>
#include <optional>
#include <unordered_map>
#include <vector>


namespace DB
{
struct Settings;
class SettingsChanges;
class SettingsConstraints;
class ASTSettingsProfileElement;
class ASTSettingsProfileElements;
class AccessControl;


struct SettingsProfileElement
{
    std::optional<UUID> parent_profile;

    String setting_name;
    Field value;
    Field min_value;
    Field max_value;
    std::optional<bool> readonly;

    auto toTuple() const { return std::tie(parent_profile, setting_name, value, min_value, max_value, readonly); }
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

private:
    void init(const ASTSettingsProfileElement & ast, const AccessControl * access_control);
};


class SettingsProfileElements : public std::vector<SettingsProfileElement>
{
public:
    SettingsProfileElements() = default;

    /// The constructor from AST requires the AccessControl if `ast.id_mode == false`.
    SettingsProfileElements(const ASTSettingsProfileElements & ast); /// NOLINT
    SettingsProfileElements(const ASTSettingsProfileElements & ast, const AccessControl & access_control);
    std::shared_ptr<ASTSettingsProfileElements> toAST() const;
    std::shared_ptr<ASTSettingsProfileElements> toASTWithNames(const AccessControl & access_control) const;

    std::vector<UUID> findDependencies() const;
    void replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids);

    void merge(const SettingsProfileElements & other);

    Settings toSettings() const;
    SettingsChanges toSettingsChanges() const;
    SettingsConstraints toSettingsConstraints(const AccessControl & access_control) const;
    std::vector<UUID> toProfileIDs() const;

    bool isBackupAllowed() const;
};

}
