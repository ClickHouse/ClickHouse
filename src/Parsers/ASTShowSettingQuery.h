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

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    String setting_name;
};

}

