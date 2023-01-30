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
            settings.writeKeyword("ID");
            settings.ostr << "(" << quoteString(str) << ")";
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
        settings.writeKeyword(use_inherit_keyword ? "INHERIT" : "PROFILE");
        settings.ostr << " ";
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
        settings.writeKeyword(" MIN ");
        settings.ostr << applyVisitor(FieldVisitorToString{}, min_value);
    }

    if (!max_value.isNull())
    {
        settings.writeKeyword(" MAX ");
        settings.ostr << applyVisitor(FieldVisitorToString{}, max_value);
    }

    if (writability)
    {
        switch (*writability)
        {
            case SettingConstraintWritability::WRITABLE:
                settings.writeKeyword(" WRITABLE");
                break;
            case SettingConstraintWritability::CONST:
                settings.writeKeyword(" CONST");
                break;
            case SettingConstraintWritability::CHANGEABLE_IN_READONLY:
                settings.writeKeyword(" CHANGEABLE_IN_READONLY");
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


void ASTSettingsProfileElements::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (empty())
    {
        settings.writeKeyword("NONE");
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
