#pragma once

#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Access/Common/SQLSecurityDefs.h>


namespace DB
{

/// DEFINER = <user_name | CURRENT_USER> SQL SECURITY <DEFINER | INVOKER | NONE>
/// If type was not set during parsing, the default type from settings will be used.
/// Currently supports only views.
class ASTSQLSecurity : public IAST
{
public:
    bool is_definer_current_user{false};
    std::shared_ptr<ASTUserNameWithHost> definer = nullptr;
    std::optional<SQLSecurityType> type = std::nullopt;

    String getID(char) const override { return "View SQL Security"; }
    ASTPtr clone() const override { return std::make_shared<ASTSQLSecurity>(*this); }

    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
