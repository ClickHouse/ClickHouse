#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/formatSettingName.h>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <base/insertAtEnd.h>


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

    void formatSettingsProfileElementsForAlter(std::string_view kind, const ASTSettingsProfileElements & elements, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        bool need_comma = false;

        size_t num_profiles = elements.getNumberOfProfiles();
        if (num_profiles > 0)
        {
            ostr << (settings.hilite ? IAST::hilite_keyword : "") << kind << " " << (num_profiles == 1 ? "PROFILE" : "PROFILES")
                 << (settings.hilite ? IAST::hilite_none : "") << " ";

            for (const auto & element : elements.elements)
            {
                if (!element->parent_profile.empty())
                {
                    if (need_comma)
                        ostr << ", ";
                    formatProfileNameOrID(element->parent_profile, /* is_id= */ false, ostr, settings);
                    need_comma = true;
                }
            }
        }

        size_t num_settings = elements.getNumberOfSettings();
        if (num_settings > 0)
        {
            if (need_comma)
                ostr << ", ";
            need_comma = false;

            ostr << (settings.hilite ? IAST::hilite_keyword : "") << kind << " " << (num_settings == 1 ? "SETTING" : "SETTINGS")
                 << (settings.hilite ? IAST::hilite_none : "") << " ";

            for (const auto & element : elements.elements)
            {
                if (!element->setting_name.empty())
                {
                    if (need_comma)
                        ostr << ", ";
                    element->format(ostr, settings);
                    need_comma = true;
                }
            }
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

size_t ASTSettingsProfileElements::getNumberOfSettings() const
{
    return std::count_if(elements.begin(), elements.end(), [](const auto & element){ return !element->setting_name.empty(); });
}

size_t ASTSettingsProfileElements::getNumberOfProfiles() const
{
    return std::count_if(elements.begin(), elements.end(), [](const auto & element){ return !element->parent_profile.empty(); });
}


ASTPtr ASTSettingsProfileElements::clone() const
{
    auto res = std::make_shared<ASTSettingsProfileElements>(*this);

    for (auto & element : res->elements)
        element = std::static_pointer_cast<ASTSettingsProfileElement>(element->clone());

    return res;
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


void ASTSettingsProfileElements::add(ASTSettingsProfileElements && other)
{
    insertAtEnd(elements, std::move(other.elements));
}


String ASTAlterSettingsProfileElements::getID(char) const
{
    return "AlterSettingsProfileElements";
}

ASTPtr ASTAlterSettingsProfileElements::clone() const
{
    auto res = std::make_shared<ASTAlterSettingsProfileElements>(*this);

    if (add_settings)
        res->add_settings = std::static_pointer_cast<ASTSettingsProfileElements>(add_settings->clone());

    if (modify_settings)
        res->modify_settings = std::static_pointer_cast<ASTSettingsProfileElements>(modify_settings->clone());

    if (drop_settings)
        res->drop_settings = std::static_pointer_cast<ASTSettingsProfileElements>(drop_settings->clone());

    return res;
}

void ASTAlterSettingsProfileElements::formatImpl(WriteBuffer & ostr, const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    bool need_comma = false;

    if (drop_all_settings)
    {
        ostr << (format.hilite ? IAST::hilite_keyword : "") << "DROP ALL SETTINGS" << (format.hilite ? IAST::hilite_none : "");
        need_comma = true;
    }

    if (drop_all_profiles)
    {
        if (need_comma)
            ostr << ", ";
        ostr << (format.hilite ? IAST::hilite_keyword : "") << "DROP ALL PROFILES" << (format.hilite ? IAST::hilite_none : "");
        need_comma = true;
    }

    if (drop_settings && !drop_settings->empty())
    {
        if (need_comma)
            ostr << ", ";
        formatSettingsProfileElementsForAlter("DROP", *drop_settings, ostr, format);
        need_comma = true;
    }

    if (add_settings && !add_settings->empty())
    {
        if (need_comma)
            ostr << ", ";
        formatSettingsProfileElementsForAlter("ADD", *add_settings, ostr, format);
        need_comma = true;
    }

    if (modify_settings && !modify_settings->empty())
    {
        if (need_comma)
            ostr << ", ";
        formatSettingsProfileElementsForAlter("MODIFY", *modify_settings, ostr, format);
    }
}

void ASTAlterSettingsProfileElements::add(ASTAlterSettingsProfileElements && other)
{
    drop_all_settings |= other.drop_all_settings;
    drop_all_profiles |= other.drop_all_profiles;

    if (other.add_settings)
    {
        if (!add_settings)
            add_settings = std::make_shared<ASTSettingsProfileElements>();
        add_settings->add(std::move(*other.add_settings));
    }

    if (other.add_settings)
    {
        if (!add_settings)
            add_settings = std::make_shared<ASTSettingsProfileElements>();
        add_settings->add(std::move(*other.add_settings));
    }

    if (other.modify_settings)
    {
        if (!modify_settings)
            modify_settings = std::make_shared<ASTSettingsProfileElements>();
        modify_settings->add(std::move(*other.modify_settings));
    }

    if (other.drop_settings)
    {
        if (!drop_settings)
            drop_settings = std::make_shared<ASTSettingsProfileElements>();
        drop_settings->add(std::move(*other.drop_settings));
    }
}

}
