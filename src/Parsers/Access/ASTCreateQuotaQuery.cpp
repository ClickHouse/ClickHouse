#include <Parsers/Access/ASTCreateQuotaQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Common/quoteString.h>
#include <Common/IntervalKind.h>
#include <base/range.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    void formatKeyType(const QuotaKeyType & key_type, const IAST::FormatSettings & settings)
    {
        const auto & type_info = QuotaKeyTypeInfo::get(key_type);
        if (key_type == QuotaKeyType::NONE)
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " NOT KEYED" << (settings.hilite ? IAST::hilite_none : "");
            return;
        }

        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " KEYED BY " << (settings.hilite ? IAST::hilite_none : "");

        if (!type_info.base_types.empty())
        {
            bool need_comma = false;
            for (const auto & base_type : type_info.base_types)
            {
                if (std::exchange(need_comma, true))
                    settings.ostr << ", ";
                settings.ostr << QuotaKeyTypeInfo::get(base_type).name;
            }
            return;
        }

        settings.ostr << type_info.name;
    }


    void formatNames(const Strings & names, const IAST::FormatSettings & settings)
    {
        settings.ostr << " ";
        bool need_comma = false;
        for (const String & name : names)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            settings.ostr << backQuoteIfNeed(name);
        }
    }


    void formatRenameTo(const String & new_name, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " RENAME TO " << (settings.hilite ? IAST::hilite_none : "")
                      << backQuote(new_name);
    }


    void formatLimit(QuotaType quota_type, QuotaValue max_value, const IAST::FormatSettings & settings)
    {
        const auto & type_info = QuotaTypeInfo::get(quota_type);
        settings.ostr << " " << type_info.name << " = " << type_info.valueToString(max_value);
    }


    void formatIntervalWithLimits(const ASTCreateQuotaQuery::Limits & limits, const IAST::FormatSettings & settings)
    {
        auto interval_kind = IntervalKind::fromAvgSeconds(limits.duration.count());
        Int64 num_intervals = limits.duration.count() / interval_kind.toAvgSeconds();

        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "")
                      << " FOR"
                      << (limits.randomize_interval ? " RANDOMIZED" : "")
                      << " INTERVAL"
                      << (settings.hilite ? IAST::hilite_none : "")
                      << " " << num_intervals << " "
                      << (settings.hilite ? IAST::hilite_keyword : "")
                      << interval_kind.toLowercasedKeyword()
                      << (settings.hilite ? IAST::hilite_none : "");

        if (limits.drop)
        {
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " NO LIMITS" << (settings.hilite ? IAST::hilite_none : "");
        }
        else
        {
            bool limit_found = false;
            for (auto quota_type : collections::range(QuotaType::MAX))
            {
                auto quota_type_i = static_cast<size_t>(quota_type);
                if (limits.max[quota_type_i])
                    limit_found = true;
            }
            if (limit_found)
            {
                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " MAX" << (settings.hilite ? IAST::hilite_none : "");
                bool need_comma = false;
                for (auto quota_type : collections::range(QuotaType::MAX))
                {
                    auto quota_type_i = static_cast<size_t>(quota_type);
                    if (limits.max[quota_type_i])
                    {
                        if (std::exchange(need_comma, true))
                            settings.ostr << ",";
                        formatLimit(quota_type, *limits.max[quota_type_i], settings);
                    }
                }
            }
            else
                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " TRACKING ONLY" << (settings.hilite ? IAST::hilite_none : "");
        }
    }

    void formatIntervalsWithLimits(const std::vector<ASTCreateQuotaQuery::Limits> & all_limits, const IAST::FormatSettings & settings)
    {
        bool need_comma = false;
        for (const auto & limits : all_limits)
        {
            if (need_comma)
                settings.ostr << ",";
            need_comma = true;

            formatIntervalWithLimits(limits, settings);
        }
    }

    void formatToRoles(const ASTRolesOrUsersSet & roles, const IAST::FormatSettings & settings)
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

    formatNames(names, settings);
    formatOnCluster(settings);

    if (!new_name.empty())
        formatRenameTo(new_name, settings);

    if (key_type)
        formatKeyType(*key_type, settings);

    formatIntervalsWithLimits(all_limits, settings);

    if (roles && (!roles->empty() || alter))
        formatToRoles(*roles, settings);
}


void ASTCreateQuotaQuery::replaceCurrentUserTag(const String & current_user_name) const
{
    if (roles)
        roles->replaceCurrentUserTag(current_user_name);
}

}
