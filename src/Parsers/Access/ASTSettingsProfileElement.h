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
    std::vector<Field> disallowed_values;
    std::optional<SettingConstraintWritability> writability;
    bool id_mode = false;  /// If true then `parent_profile` keeps UUID, not a name.
    bool use_inherit_keyword = false;  /// If true then this element is a part of ASTCreateSettingsProfileQuery.

    bool empty() const { return parent_profile.empty() && setting_name.empty(); }

    String getID(char) const override { return "SettingsProfileElement"; }
    ASTPtr clone() const override { return std::make_shared<ASTSettingsProfileElement>(*this); }

protected:
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

    size_t getNumberOfSettings() const;
    size_t getNumberOfProfiles() const;

    String getID(char) const override { return "SettingsProfileElements"; }
    ASTPtr clone() const override;

    void setUseInheritKeyword(bool use_inherit_keyword_);

    void add(ASTSettingsProfileElements && other);

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

/* Represents a clause used to alter settings or profiles assigned to a user or a role or another profile.
 *     [ADD|MODIFY SETTINGS variable [=value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
 *     [DROP SETTINGS variable [,...] ]
 *     [ADD PROFILES 'profile_name' [,...] ]
 *     [DROP PROFILES 'profile_name' [,...] ]
 *     [DROP ALL PROFILES]
 *     [DROP ALL SETTINGS]
*/
class ASTAlterSettingsProfileElements : public IAST
{
public:
    std::shared_ptr<ASTSettingsProfileElements> add_settings;
    std::shared_ptr<ASTSettingsProfileElements> modify_settings;
    std::shared_ptr<ASTSettingsProfileElements> drop_settings;

    bool drop_all_settings = false;
    bool drop_all_profiles = false;

    String getID(char) const override;
    ASTPtr clone() const override;

    void add(ASTAlterSettingsProfileElements && other);

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};

}
