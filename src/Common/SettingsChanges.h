#pragma once
#include <Core/Field.h>


namespace DB
{

class IColumn;


struct SettingValue
{
    virtual ~SettingValue() = default;
    virtual const Field & getField() const = 0;
    virtual Field & getField() = 0;
    virtual std::string toString() const = 0;
};
using SettingValuePtr = std::shared_ptr<SettingValue>;

SettingValuePtr getSettingValueFromField(const Field & field);
SettingValuePtr getSettingValueFromField(Field && field);


class SettingChange
{
private:
    String name;
    SettingValuePtr value;

public:
    SettingChange() = default;

    SettingChange(std::string_view name_, const Field & value_) : name(name_), value(getSettingValueFromField(value_)) {}
    SettingChange(std::string_view name_, Field && value_) : name(name_), value(getSettingValueFromField(std::move(value_))) {}
    SettingChange(std::string_view name_, SettingValuePtr && value_) : name(name_), value(std::move(value_)) {}

    friend bool operator ==(const SettingChange & lhs, const SettingChange & rhs)
    {
        return (lhs.name == rhs.name) && (lhs.value == rhs.value);
    }

    friend bool operator !=(const SettingChange & lhs, const SettingChange & rhs) { return !(lhs == rhs); }

    const String & getName() const { return name; }
    String & getName() { return name; }

    SettingValuePtr getValue() const { return value; }
    const Field & getFieldValue() const { return value->getField(); }
    Field & getFieldValue() { return value->getField(); }

    void setValue(const Field & field) { value = getSettingValueFromField(field); }
    void setValue(SettingValuePtr value_) { value = std::move(value_); }

    String getValueString() const { return value->toString(); }
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
