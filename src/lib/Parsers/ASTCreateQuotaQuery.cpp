#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/ASTRoleList.h>
#include <Common/quoteString.h>
#include <Common/IntervalKind.h>
#include <ext/range.h>


namespace DB
{
namespace
{
    using KeyType = Quota::KeyType;
    using ResourceType = Quota::ResourceType;
    using ResourceAmount = Quota::ResourceAmount;


    void formatKeyType(const KeyType & key_type, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " KEYED BY " << (settings.hilite ? IAST::hilite_none : "") << "'"
                      << Quota::getNameOfKeyType(key_type) << "'";
    }


    void formatRenameTo(const String & new_name, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " RENAME TO " << (settings.hilite ? IAST::hilite_none : "")
                      << backQuote(new_name);
    }


    void formatLimit(ResourceType resource_type, ResourceAmount max, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " MAX " << Quota::resourceTypeToKeyword(resource_type)
                      << (settings.hilite ? IAST::hilite_none : "");

        settings.ostr << (settings.hilite ? IAST::hilite_operator : "") << " = " << (settings.hilite ? IAST::hilite_none : "");

        if (max == Quota::UNLIMITED)
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "ANY" << (settings.hilite ? IAST::hilite_none : "");
        else if (resource_type == Quota::EXECUTION_TIME)
            settings.ostr << Quota::executionTimeToSeconds(max);
        else
            settings.ostr << max;
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

        if (limits.unset_tracking)
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " UNSET TRACKING" << (settings.hilite ? IAST::hilite_none : "");
        }
        else
        {
            bool limit_found = false;
            for (auto resource_type : ext::range_with_static_cast<ResourceType>(Quota::MAX_RESOURCE_TYPE))
            {
                if (limits.max[resource_type])
                {
                    if (limit_found)
                        settings.ostr << ",";
                    limit_found = true;
                    formatLimit(resource_type, *limits.max[resource_type], settings);
                }
            }
            if (!limit_found)
                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " TRACKING" << (settings.hilite ? IAST::hilite_none : "");
        }
    }

    void formatAllLimits(const std::vector<ASTCreateQuotaQuery::Limits> & all_limits, const IAST::FormatSettings & settings)
    {
        bool need_comma = false;
        for (auto & limits : all_limits)
        {
            if (need_comma)
                settings.ostr << ",";
            need_comma = true;

            formatLimits(limits, settings);
        }
    }

    void formatRoles(const ASTRoleList & roles, const IAST::FormatSettings & settings)
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
    settings.ostr << (settings.hilite ? hilite_keyword : "") << (alter ? "ALTER QUOTA" : "CREATE QUOTA")
                  << (settings.hilite ? hilite_none : "");

    if (if_exists)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " IF EXISTS" << (settings.hilite ? hilite_none : "");
    else if (if_not_exists)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " IF NOT EXISTS" << (settings.hilite ? hilite_none : "");
    else if (or_replace)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " OR REPLACE" << (settings.hilite ? hilite_none : "");

    settings.ostr << " " << backQuoteIfNeed(name);

    if (!new_name.empty())
        formatRenameTo(new_name, settings);

    if (key_type)
        formatKeyType(*key_type, settings);

    formatAllLimits(all_limits, settings);

    if (roles)
        formatRoles(*roles, settings);
}
}
