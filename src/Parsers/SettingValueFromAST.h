#pragma once
#include <Parsers/formatAST.h>
#include <Common/SettingsChanges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_GET_SETTING_VALUE;
}

struct SettingValueFromAST : SettingValue
{
    explicit SettingValueFromAST(const ASTPtr & value_) : value(value_) {}

    ASTPtr value;
    std::optional<Field> field;

    [[noreturn]] void throwNoValue() const
    {
        throw Exception(
            ErrorCodes::CANNOT_GET_SETTING_VALUE,
            "Cannot get setting value, it must be converted from AST to Field first");
    }

    const Field & getField() const override
    {
        if (field)
            return *field;
        throwNoValue();
    }

    Field & getField() override
    {
        if (field)
            return *field;
        throwNoValue();
    }
    void setField(const Field & field_) { field = field_; }
    std::string toString() const override { return serializeAST(*value); }
};

}
