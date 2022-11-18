#pragma once

#include <Core/Field.h>
#include <Parsers/IAST.h>


namespace DB
{

class IColumn;

class SettingChange
{
private:
    String name;
    Field field_value;
    ASTPtr ast_value; /// A setting value which cannot be put in Field.

public:
    SettingChange() = default;

    SettingChange(std::string_view name_, const Field & value_) : name(name_), field_value(value_) {}
    SettingChange(std::string_view name_, Field && value_) : name(name_), field_value(std::move(value_)) {}
    SettingChange(std::string_view name_, const ASTPtr & value_) : name(name_), ast_value(value_->clone()) {}

    friend bool operator ==(const SettingChange & lhs, const SettingChange & rhs)
    {
        return (lhs.name == rhs.name) && (lhs.field_value == rhs.field_value) && (lhs.ast_value == rhs.ast_value);
    }

    friend bool operator !=(const SettingChange & lhs, const SettingChange & rhs) { return !(lhs == rhs); }

    void throwIfASTValueNotConvertedToField() const;

    const String & getName() const { return name; }
    String & getName() { return name; }

    const Field & getFieldValue() const;
    Field & getFieldValue();

    const ASTPtr & getASTValue() const { return ast_value; }
    ASTPtr & getASTValue() { return ast_value; }

    void setFieldValue(const Field & field);
    void setASTValue(const ASTPtr & ast);

    String getValueString() const;
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
