#pragma once

#include <Parsers/IAST.h>
#include <Core/Field.h>
#include <Common/SettingConstraintWritability.h>

namespace DB
{
/** Represents a settings profile's element like the following
  * {variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY]} | PROFILE 'profile_name'
  */
class ASTSettingsProfileElement : public IAST
{
public:
    String parent_profile;
    String setting_name;
    std::optional<Field> value;
    std::optional<Field> min_value;
    std::optional<Field> max_value;
    std::optional<SettingConstraintWritability> writability;
    bool id_mode = false;  /// If true then `parent_profile` keeps UUID, not a name.
    bool use_inherit_keyword = false;  /// If true then this element is a part of ASTCreateSettingsProfileQuery.

    bool empty() const { return parent_profile.empty() && setting_name.empty(); }

    String getID(char) const override { return "SettingsProfileElement"; }
    ASTPtr clone() const override { return std::make_shared<ASTSettingsProfileElement>(*this); }
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};


/** Represents settings profile's elements like the following
  * {{variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY]} | PROFILE 'profile_name'} [,...]
  */
class ASTSettingsProfileElements : public IAST
{
public:
    std::vector<std::shared_ptr<ASTSettingsProfileElement>> elements;

    bool empty() const;

    String getID(char) const override { return "SettingsProfileElements"; }
    ASTPtr clone() const override { return std::make_shared<ASTSettingsProfileElements>(*this); }
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

    void setUseInheritKeyword(bool use_inherit_keyword_);
};
}
