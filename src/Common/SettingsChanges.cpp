#include <Common/SettingsChanges.h>
#include <Parsers/formatAST.h>
#include <Common/FieldVisitorToString.h>

namespace DB
{

namespace
{
    SettingChange * find(SettingsChanges & changes, std::string_view name)
    {
        auto it = std::find_if(changes.begin(), changes.end(), [&name](const SettingChange & change) { return change.getName() == name; });
        if (it == changes.end())
            return nullptr;
        return &*it;
    }

    const SettingChange * find(const SettingsChanges & changes, std::string_view name)
    {
        auto it = std::find_if(changes.begin(), changes.end(), [&name](const SettingChange & change) { return change.getName() == name; });
        if (it == changes.end())
            return nullptr;
        return &*it;
    }
}

bool SettingsChanges::tryGet(std::string_view name, Field & out_value) const
{
    const auto * change = find(*this, name);
    if (!change)
        return false;
    out_value = change->getFieldValue();
    return true;
}

const Field * SettingsChanges::tryGet(std::string_view name) const
{
    const auto * change = find(*this, name);
    if (!change)
        return nullptr;
    return &change->getFieldValue();
}

Field * SettingsChanges::tryGet(std::string_view name)
{
    auto * change = find(*this, name);
    if (!change)
        return nullptr;
    return &change->getFieldValue();
}

struct SettingValueFromField : SettingValue
{
    explicit SettingValueFromField(const Field & value_) : value(value_) {}
    explicit SettingValueFromField(Field && value_) : value(std::move(value_)) {}

    const Field & getField() const override { return value; }
    Field & getField() override { return value; }
    std::string toString() const override { return applyVisitor(FieldVisitorToString(), value); }

    Field value;
};

SettingValuePtr getSettingValueFromField(const Field & field)
{
    return std::make_shared<SettingValueFromField>(field);
}

SettingValuePtr getSettingValueFromField(Field && field)
{
    return std::make_shared<SettingValueFromField>(std::move(field));
}

}
