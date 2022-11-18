#pragma once

#include <Core/Field.h>
#include <Parsers/IAST.h>


namespace DB
{

class IColumn;

struct SettingChange
{
    String name;
    Field value;

    /// A setting value which cannot be put in Field.
    ASTPtr value_ast;

    SettingChange() = default;

    SettingChange(std::string_view name_, const Field & value_) : name(name_), value(value_) {}
    SettingChange(std::string_view name_, Field && value_) : name(name_), value(std::move(value_)) {}
    SettingChange(std::string_view name_, const ASTPtr & value_) : name(name_), value_ast(value_->clone()) {}

    friend bool operator ==(const SettingChange & lhs, const SettingChange & rhs) { return (lhs.name == rhs.name) && (lhs.value == rhs.value) && (lhs.value_ast == rhs.value_ast); }
    friend bool operator !=(const SettingChange & lhs, const SettingChange & rhs) { return !(lhs == rhs); }
};


class SettingsChanges : public std::vector<SettingChange>
{
public:
    using std::vector<SettingChange>::vector;

    bool tryGet(std::string_view name, Field & out_value) const;
    const Field * tryGet(std::string_view name) const;
    Field * tryGet(std::string_view name);
};

}
