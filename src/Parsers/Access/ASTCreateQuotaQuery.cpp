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
            settings.writeKeyword(" NOT KEYED");
            return;
        }

        settings.writeKeyword(" KEYED BY ");

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
        settings.writeKeyword(" RENAME TO ");
        settings.ostr << backQuote(new_name);
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

        settings.writeKeyword(" FOR");
        settings.writeKeyword(limits.randomize_interval ? " RANDOMIZED" : "");
        settings.writeKeyword(" INTERVAL");
        settings.ostr << " " << num_intervals << " ";
        settings.writeKeyword(interval_kind.toLowercasedKeyword());

        if (limits.drop)
        {
            settings.writeKeyword(" NO LIMITS");
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
                settings.writeKeyword(" MAX");
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
                settings.writeKeyword(" TRACKING ONLY");
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
        settings.writeKeyword(" TO ");
        roles.format(settings);
    }
}


String ASTCreateQuotaQuery::getID(char) const
{
    return "CreateQuotaQuery";
}


ASTPtr ASTCreateQuotaQuery::clone() const
{
    auto res = std::make_shared<ASTCreateQuotaQuery>(*this);

    if (roles)
        res->roles = std::static_pointer_cast<ASTRolesOrUsersSet>(roles->clone());

    return res;
}


void ASTCreateQuotaQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (attach)
    {
        settings.writeKeyword("ATTACH QUOTA");
    }
    else
    {
        settings.writeKeyword(alter ? "ALTER QUOTA" : "CREATE QUOTA");
    }

    if (if_exists)
        settings.writeKeyword(" IF EXISTS");
    else if (if_not_exists)
        settings.writeKeyword(" IF NOT EXISTS");
    else if (or_replace)
        settings.writeKeyword(" OR REPLACE");

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
