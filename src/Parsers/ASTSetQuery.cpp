#include <Parsers/ASTSetQuery.h>
#include <Parsers/formatSettingName.h>
#include <Common/SipHash.h>
#include <Common/FieldVisitorHash.h>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

class FieldVisitorToSetting : public StaticVisitor<String>
{
public:
    template <class T>
    String operator() (const T & x) const
    {
        FieldVisitorToString visitor;
        return visitor(x);
    }

    String operator() (const Map & x) const
    {
        WriteBufferFromOwnString wb;

        wb << '{';

        auto it = x.begin();
        while (it != x.end())
        {
            if (it != x.begin())
                wb << ", ";
            wb << applyVisitor(*this, *it);
            ++it;
        }
        wb << '}';

        return wb.str();
    }

    String operator() (const Tuple & x) const
    {
        WriteBufferFromOwnString wb;

        for (auto it = x.begin(); it != x.end(); ++it)
        {
            if (it != x.begin())
                wb << ":";
            wb << applyVisitor(*this, *it);
        }

        return wb.str();
    }
};


void ASTSetQuery::updateTreeHashImpl(SipHash & hash_state, bool /*ignore_aliases*/) const
{
    for (const auto & change : changes)
    {
        hash_state.update(change.name.size());
        hash_state.update(change.name);
        applyVisitor(FieldVisitorHash(hash_state), change.value);
    }
}

void ASTSetQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & format, FormatState &, FormatStateStacked state) const
{
    if (is_standalone)
        ostr << (format.hilite ? hilite_keyword : "") << "SET " << (format.hilite ? hilite_none : "");

    bool first = true;

    for (const auto & change : changes)
    {
        if (!first)
            ostr << ", ";
        else
            first = false;

        formatSettingName(change.name, ostr);

        auto format_if_secret = [&]() -> bool
        {
            CustomType custom;
            if (change.value.tryGet<CustomType>(custom) && custom.isSecret())
            {
                ostr << " = " << custom.toString(/* show_secrets */false);
                return true;
            }

            if (state.create_engine_name == "Iceberg")
            {
                const std::set<std::string_view> secret_settings = {"catalog_credential", "auth_header"};
                if (secret_settings.contains(change.name))
                {
                    ostr << " = " << "'[HIDDEN]'";
                    return true;
                }
            }

            return false;
        };

        if (format.show_secrets || !format_if_secret())
            ostr << " = " << applyVisitor(FieldVisitorToSetting(), change.value);
    }

    for (const auto & setting_name : default_settings)
    {
        if (!first)
            ostr << ", ";
        else
            first = false;

        formatSettingName(setting_name, ostr);
        ostr << " = DEFAULT";
    }

    for (const auto & [name, value] : query_parameters)
    {
        if (!first)
            ostr << ", ";
        else
            first = false;

        formatSettingName(QUERY_PARAMETER_NAME_PREFIX + name, ostr);
        ostr << " = " << quoteString(value);
    }
}

void ASTSetQuery::appendColumnName(WriteBuffer & ostr) const
{
    Hash hash = getTreeHash(/*ignore_aliases=*/ true);

    writeCString("__settings_", ostr);
    writeText(hash.low64, ostr);
    ostr.write('_');
    writeText(hash.high64, ostr);
}

}
