#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/ASTExtendedRoleSet.h>
#include <Common/quoteString.h>
#include <Common/IntervalKind.h>
#include <ext/range.h>


namespace DB
{
namespace
{
    using KeyType = Quota::KeyType;
    using KeyTypeInfo = Quota::KeyTypeInfo;
    using ResourceType = Quota::ResourceType;
    using ResourceTypeInfo = Quota::ResourceTypeInfo;
    using ResourceAmount = Quota::ResourceAmount;


    void formatKeyType(const KeyType & key_type, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " KEYED BY " << (settings.hilite ? IAST::hilite_none : "") << "'"
                      << KeyTypeInfo::get(key_type).name << "'";
    }


    void formatRenameTo(const String & new_name, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " RENAME TO " << (settings.hilite ? IAST::hilite_none : "")
                      << backQuote(new_name);
    }


    void formatLimit(ResourceType resource_type, ResourceAmount max, bool first, const IAST::FormatSettings & settings)
    {
        if (first)
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " MAX" << (settings.hilite ? IAST::hilite_none : "");
        else
            settings.ostr << ",";

        const auto & type_info = ResourceTypeInfo::get(resource_type);
        settings.ostr << " " << (settings.hilite ? IAST::hilite_keyword : "") << type_info.keyword
                      << (settings.hilite ? IAST::hilite_none : "") << " " << type_info.amountToString(max);
    }


    void formatLimits(const ASTCreateQuotaQuery::Limits & limits, const IAST::FormatSettings & settings)
    {
        auto interval_kind = IntervalKind::fromAvgSeconds(limits.duration.count());
        Int64 num_intervals = limits.duration.count() / interval_kind.toAvgSeconds();

        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "")
                      << " FOR"
                      << (limits.randomize_interval ? " RANDOMIZED" : "")
                      << " INTERVAL "
                      << (settings.hilite ? IAST::hilite_none : "")
                      << num_intervals << " "
                      << (settings.hilite ? IAST::hilite_keyword : "")
                      << interval_kind.toKeyword()
                      << (settings.hilite ? IAST::hilite_none : "");

        if (limits.drop)
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " NO LIMITS" << (settings.hilite ? IAST::hilite_none : "");
        }
        else
        {
            bool limit_found = false;
            for (auto resource_type : ext::range(Quota::MAX_RESOURCE_TYPE))
            {
                if (limits.max[resource_type])
                {
                    formatLimit(resource_type, *limits.max[resource_type], !limit_found, settings);
                    limit_found = true;
                }
            }
            if (!limit_found)
                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " TRACKING ONLY" << (settings.hilite ? IAST::hilite_none : "");
        }
    }

    void formatAllLimits(const std::vector<ASTCreateQuotaQuery::Limits> & all_limits, const IAST::FormatSettings & settings)
    {
        bool need_comma = false;
        for (const auto & limits : all_limits)
        {
            if (need_comma)
                settings.ostr << ",";
            need_comma = true;

            formatLimits(limits, settings);
        }
    }

    void formatToRoles(const ASTExtendedRoleSet & roles, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " TO " << (settings.hilite ? IAST::hilite_none : "");
        roles.format(settings);
    }
}


String ASTCreateQuotaQuery::getID(char) const
{
    return "CreateQuotaQuery";
}


ASTPtr ASTCreateQuotaQuery::clone() const
{
    return std::make_shared<ASTCreateQuotaQuery>(*this);
}


void ASTCreateQuotaQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (attach)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "ATTACH QUOTA" << (settings.hilite ? hilite_none : "");
    }
    else
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (alter ? "ALTER QUOTA" : "CREATE QUOTA")
                      << (settings.hilite ? hilite_none : "");
    }

    if (if_exists)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " IF EXISTS" << (settings.hilite ? hilite_none : "");
    else if (if_not_exists)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " IF NOT EXISTS" << (settings.hilite ? hilite_none : "");
    else if (or_replace)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " OR REPLACE" << (settings.hilite ? hilite_none : "");

    settings.ostr << " " << backQuoteIfNeed(name);

    formatOnCluster(settings);

    if (!new_name.empty())
        formatRenameTo(new_name, settings);

    if (key_type)
        formatKeyType(*key_type, settings);

    formatAllLimits(all_limits, settings);

    if (roles && (!roles->empty() || alter))
        formatToRoles(*roles, settings);
}


void ASTCreateQuotaQuery::replaceCurrentUserTagWithName(const String & current_user_name) const
{
    if (roles)
        roles->replaceCurrentUserTagWithName(current_user_name);
}

}
