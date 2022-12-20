#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/formatSettingName.h>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    void formatProfileNameOrID(const String & str, bool is_id, const IAST::FormatSettings & settings)
    {
        if (is_id)
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "ID" << (settings.hilite ? IAST::hilite_none : "") << "("
                          << quoteString(str) << ")";
        }
        else
        {
            settings.ostr << backQuoteIfNeed(str);
        }
    }
}

void ASTSettingsProfileElement::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (!parent_profile.empty())
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << (use_inherit_keyword ? "INHERIT" : "PROFILE") << " "
                      << (settings.hilite ? IAST::hilite_none : "");
        formatProfileNameOrID(parent_profile, id_mode, settings);
        return;
    }

    formatSettingName(setting_name, settings.ostr);

    if (!value.isNull())
    {
        settings.ostr << " = " << applyVisitor(FieldVisitorToString{}, value);
    }

    if (!min_value.isNull())
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " MIN " << (settings.hilite ? IAST::hilite_none : "")
                      << applyVisitor(FieldVisitorToString{}, min_value);
    }

    if (!max_value.isNull())
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " MAX " << (settings.hilite ? IAST::hilite_none : "")
                      << applyVisitor(FieldVisitorToString{}, max_value);
    }

    if (readonly)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << (*readonly ? " READONLY" : " WRITABLE")
                      << (settings.hilite ? IAST::hilite_none : "");
    }
}


bool ASTSettingsProfileElements::empty() const
{
    for (const auto & element : elements)
        if (!element->empty())
            return false;
    return true;
}


void ASTSettingsProfileElements::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (empty())
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "NONE" << (settings.hilite ? IAST::hilite_none : "");
        return;
    }

    bool need_comma = false;
    for (const auto & element : elements)
    {
        if (need_comma)
            settings.ostr << ", ";
        need_comma = true;

        element->format(settings);
    }
}


void ASTSettingsProfileElements::setUseInheritKeyword(bool use_inherit_keyword_)
{
    for (auto & element : elements)
        element->use_inherit_keyword = use_inherit_keyword_;
}

}
