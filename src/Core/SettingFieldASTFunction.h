#pragma once

#include <Core/SettingsFields.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

/// Represents ASTFunction, can be parsed from string,
/// outputs to string with secrets (so it can be parsed back).
struct SettingFieldASTFunction
{
    boost::intrusive_ptr<ASTFunction> value;
    bool changed = false;

    explicit SettingFieldASTFunction(const boost::intrusive_ptr<ASTFunction> & func = {});
    explicit SettingFieldASTFunction(const String & str);
    explicit SettingFieldASTFunction(const Field & f);

    SettingFieldASTFunction(const SettingFieldASTFunction &) = default;
    SettingFieldASTFunction & operator =(const SettingFieldASTFunction &) = default;

    SettingFieldASTFunction & operator =(const boost::intrusive_ptr<ASTFunction> & func) { value = func; changed = true; return *this; }
    SettingFieldASTFunction & operator =(const String & str);
    SettingFieldASTFunction & operator =(const Field & f);

    bool isChanged() const { return changed; }
    void setChanged(bool changed_) { changed = changed_; }

    operator const boost::intrusive_ptr<ASTFunction> &() const { return value; } /// NOLINT
    explicit operator bool() const { return value != nullptr; }
    explicit operator Field() const { return toString(); }

    String toString() const;
    void parseFromString(const String & str);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};

}
