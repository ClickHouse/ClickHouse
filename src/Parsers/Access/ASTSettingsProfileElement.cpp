#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/formatSettingName.h>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    void formatProfileNameOrID(const String & str, bool is_id, const IAST::FormattingBuffer & out)
    {
        if (is_id)
        {
            out.writeKeyword("ID");
            out.ostr << "(" << quoteString(str) << ")";
        }
        else
        {
            out.ostr << backQuoteIfNeed(str);
        }
    }
}

void ASTSettingsProfileElement::formatImpl(const FormattingBuffer & out) const
{
    if (!parent_profile.empty())
    {
        out.writeKeyword(use_inherit_keyword ? "INHERIT" : "PROFILE");
        out.ostr << " ";
        formatProfileNameOrID(parent_profile, id_mode, out);
        return;
    }

    formatSettingName(setting_name, out.ostr);

    if (!value.isNull())
    {
        out.ostr << " = " << applyVisitor(FieldVisitorToString{}, value);
    }

    if (!min_value.isNull())
    {
        out.writeKeyword(" MIN ");
        out.ostr << applyVisitor(FieldVisitorToString{}, min_value);
    }

    if (!max_value.isNull())
    {
        out.writeKeyword(" MAX ");
        out.ostr << applyVisitor(FieldVisitorToString{}, max_value);
    }

    if (writability)
    {
        switch (*writability)
        {
            case SettingConstraintWritability::WRITABLE:
                out.writeKeyword(" WRITABLE");
                break;
            case SettingConstraintWritability::CONST:
                out.writeKeyword(" CONST");
                break;
            case SettingConstraintWritability::CHANGEABLE_IN_READONLY:
                out.writeKeyword(" CHANGEABLE_IN_READONLY");
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


void ASTSettingsProfileElements::formatImpl(const FormattingBuffer & out) const
{
    if (empty())
    {
        out.writeKeyword("NONE");
        return;
    }

    bool need_comma = false;
    for (const auto & element : elements)
    {
        if (need_comma)
            out.ostr << ", ";
        need_comma = true;

        element->format(out.copy());
    }
}


void ASTSettingsProfileElements::setUseInheritKeyword(bool use_inherit_keyword_)
{
    for (auto & element : elements)
        element->use_inherit_keyword = use_inherit_keyword_;
}

}
