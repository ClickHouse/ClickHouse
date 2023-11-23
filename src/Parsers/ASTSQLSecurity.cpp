
#include <Parsers/ASTSQLSecurity.h>
#include <IO/Operators.h>

namespace DB
{

void ASTSQLSecurity::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (!type.has_value())
        return;

    if (definer || is_definer_current_user)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "DEFINER" << (settings.hilite ? hilite_none : "");
        settings.ostr << " = ";
        if (definer)
            definer->formatImpl(settings, state, frame);
        else
            settings.ostr << "CURRENT_USER";
        settings.ostr << " ";
    }

    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SQL SECURITY" << (settings.hilite ? hilite_none : "");
    switch (*type)
    {
        case Type::INVOKER:
            settings.ostr << " INVOKER";
            break;
        case Type::DEFINER:
            settings.ostr << " DEFINER";
            break;
        case Type::NONE:
            settings.ostr << " NONE";
            break;
    }
}

}
