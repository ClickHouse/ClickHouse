
#include <Parsers/ASTSQLSecurity.h>
#include <IO/Operators.h>

namespace DB
{

void ASTSQLSecurity::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (!type)
        return;

    if (definer || is_definer_current_user)
    {
        ostr << "DEFINER";
        ostr << " = ";
        if (definer)
            definer->format(ostr, settings, state, frame);
        else
            ostr << "CURRENT_USER";
        ostr << " ";
    }

    ostr << "SQL SECURITY";
    switch (*type)
    {
        case SQLSecurityType::INVOKER:
            ostr << " INVOKER";
            break;
        case SQLSecurityType::DEFINER:
            ostr << " DEFINER";
            break;
        case SQLSecurityType::NONE:
            ostr << " NONE";
            break;
    }
}

}
