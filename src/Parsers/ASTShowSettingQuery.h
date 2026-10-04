#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

/// Query SHOW SETTING setting_name
class ASTShowSettingQuery : public ASTQueryWithOutput
{
public:
    explicit ASTShowSettingQuery(String setting_name_)
        : setting_name(setting_name_)
    {}

    const String & getSettingName() const { return setting_name; }

    String getID(char) const override { return "ShowSetting"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Show; }

    /// The selected `setting_name` is a plain member, not part of `children`, and `getID` is a
    /// constant. Fold it into the hash so the rewrite-rule matcher (which treats an equal tree hash
    /// as semantic equality) does not let a rule template for `SHOW SETTING a` over-match
    /// `SHOW SETTING b`.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    String setting_name;
};

}

