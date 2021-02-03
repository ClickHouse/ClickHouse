#pragma once

#include <Core/Field.h>
#include <Core/UUID.h>
#include <optional>
#include <vector>


namespace DB
{
struct Settings;
class SettingsChanges;
class SettingsConstraints;
class ASTSettingsProfileElement;
class ASTSettingsProfileElements;
class AccessControlManager;


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

    SettingsProfileElement() {}

    /// The constructor from AST requires the AccessControlManager if `ast.id_mode == false`.
    SettingsProfileElement(const ASTSettingsProfileElement & ast);
    SettingsProfileElement(const ASTSettingsProfileElement & ast, const AccessControlManager & manager);
    std::shared_ptr<ASTSettingsProfileElement> toAST() const;
    std::shared_ptr<ASTSettingsProfileElement> toASTWithNames(const AccessControlManager & manager) const;

private:
    void init(const ASTSettingsProfileElement & ast, const AccessControlManager * manager);
};


class SettingsProfileElements : public std::vector<SettingsProfileElement>
{
public:
    SettingsProfileElements() {}

    /// The constructor from AST requires the AccessControlManager if `ast.id_mode == false`.
    SettingsProfileElements(const ASTSettingsProfileElements & ast);
    SettingsProfileElements(const ASTSettingsProfileElements & ast, const AccessControlManager & manager);
    std::shared_ptr<ASTSettingsProfileElements> toAST() const;
    std::shared_ptr<ASTSettingsProfileElements> toASTWithNames(const AccessControlManager & manager) const;

    void merge(const SettingsProfileElements & other);

    Settings toSettings() const;
    SettingsChanges toSettingsChanges() const;
    SettingsConstraints toSettingsConstraints(const AccessControlManager & manager) const;
};

}
