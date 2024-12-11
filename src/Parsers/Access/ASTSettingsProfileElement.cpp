#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/formatSettingName.h>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    void formatProfileNameOrID(const String & str, bool is_id, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        if (is_id)
        {
            ostr << (settings.hilite ? IAST::hilite_keyword : "") << "ID" << (settings.hilite ? IAST::hilite_none : "") << "("
                          << quoteString(str) << ")";
        }
        else
        {
            ostr << backQuote(str);
        }
    }
}

void ASTSettingsProfileElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (!parent_profile.empty())
    {
        ostr << (settings.hilite ? IAST::hilite_keyword : "") << (use_inherit_keyword ? "INHERIT" : "PROFILE") << " "
                      << (settings.hilite ? IAST::hilite_none : "");
        formatProfileNameOrID(parent_profile, id_mode, ostr, settings);
        return;
    }

    formatSettingName(setting_name, ostr);

    if (value)
    {
        ostr << " = " << applyVisitor(FieldVisitorToString{}, *value);
    }

    if (min_value)
    {
        ostr << (settings.hilite ? IAST::hilite_keyword : "") << " MIN " << (settings.hilite ? IAST::hilite_none : "")
                      << applyVisitor(FieldVisitorToString{}, *min_value);
    }

    if (max_value)
    {
        ostr << (settings.hilite ? IAST::hilite_keyword : "") << " MAX " << (settings.hilite ? IAST::hilite_none : "")
                      << applyVisitor(FieldVisitorToString{}, *max_value);
    }

    if (writability)
    {
        switch (*writability)
        {
            case SettingConstraintWritability::WRITABLE:
                ostr << (settings.hilite ? IAST::hilite_keyword : "") << " WRITABLE"
                            << (settings.hilite ? IAST::hilite_none : "");
                break;
            case SettingConstraintWritability::CONST:
                ostr << (settings.hilite ? IAST::hilite_keyword : "") << " CONST"
                            << (settings.hilite ? IAST::hilite_none : "");
                break;
            case SettingConstraintWritability::CHANGEABLE_IN_READONLY:
                ostr << (settings.hilite ? IAST::hilite_keyword : "") << " CHANGEABLE_IN_READONLY"
                            << (settings.hilite ? IAST::hilite_none : "");
                break;
            case SettingConstraintWritability::MAX: break;
        }
    }
}


bool ASTSettingsProfileElements::empty() const
{
    for (const auto & element : elements)
        if (!element->empty())
            return false;
    return true;
}


void ASTSettingsProfileElements::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (empty())
    {
        ostr << (settings.hilite ? IAST::hilite_keyword : "") << "NONE" << (settings.hilite ? IAST::hilite_none : "");
        return;
    }

    bool need_comma = false;
    for (const auto & element : elements)
    {
        if (need_comma)
            ostr << ", ";
        need_comma = true;

        element->format(ostr, settings);
    }
}


void ASTSettingsProfileElements::setUseInheritKeyword(bool use_inherit_keyword_)
{
    for (auto & element : elements)
        element->use_inherit_keyword = use_inherit_keyword_;
}

}
