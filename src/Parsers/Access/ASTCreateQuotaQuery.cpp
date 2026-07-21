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
    void formatKeyType(const QuotaKeyType & key_type, WriteBuffer & ostr, const IAST::FormatSettings &)
    {
        const auto & type_info = QuotaKeyTypeInfo::get(key_type);
        if (key_type == QuotaKeyType::NONE)
        {
            ostr << " NOT KEYED";
            return;
        }

        ostr << " KEYED BY ";

        if (!type_info.base_types.empty())
        {
            bool need_comma = false;
            for (const auto & base_type : type_info.base_types)
            {
                if (std::exchange(need_comma, true))
                    ostr << ", ";
                ostr << QuotaKeyTypeInfo::get(base_type).name;
            }
            return;
        }

        ostr << type_info.name;
    }


    void formatNames(const Strings & names, WriteBuffer & ostr)
    {
        ostr << " ";
        bool need_comma = false;
        for (const String & name : names)
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            ostr << backQuoteIfNeed(name);
        }
    }


    void formatRenameTo(const String & new_name, WriteBuffer & ostr, const IAST::FormatSettings &)
    {
        ostr << " RENAME TO " << backQuote(new_name);
    }


    void formatLimit(QuotaType quota_type, QuotaValue max_value, WriteBuffer & ostr)
    {
        const auto & type_info = QuotaTypeInfo::get(quota_type);
        ostr << " " << type_info.name << " = " << type_info.valueToString(max_value);
    }


    void formatIntervalWithLimits(const ASTCreateQuotaQuery::Limits & limits, WriteBuffer & ostr, const IAST::FormatSettings &)
    {
        auto interval_kind = IntervalKind::fromAvgSeconds(limits.duration.count());
        Int64 num_intervals = limits.duration.count() / interval_kind.toAvgSeconds();

        ostr << " FOR" << (limits.randomize_interval ? " RANDOMIZED" : "") << " INTERVAL"
            << " " << num_intervals << " " << interval_kind.toLowercasedKeyword();

        if (limits.drop)
        {
            ostr << " NO LIMITS";
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
                ostr << " MAX";
                bool need_comma = false;
                for (auto quota_type : collections::range(QuotaType::MAX))
                {
                    auto quota_type_i = static_cast<size_t>(quota_type);
                    if (limits.max[quota_type_i])
                    {
                        if (std::exchange(need_comma, true))
                            ostr << ",";
                        formatLimit(quota_type, *limits.max[quota_type_i], ostr);
                    }
                }
            }
            else
                ostr << " TRACKING ONLY";
        }
    }

    void formatIntervalsWithLimits(const std::vector<ASTCreateQuotaQuery::Limits> & all_limits, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        bool need_comma = false;
        for (const auto & limits : all_limits)
        {
            if (need_comma)
                ostr << ",";
            need_comma = true;

            formatIntervalWithLimits(limits, ostr, settings);
        }
    }

    void formatToRoles(const ASTRolesOrUsersSet & roles, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << " TO ";
        roles.format(ostr, settings);
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


void ASTCreateQuotaQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (attach)
    {
        ostr << "ATTACH QUOTA";
    }
    else
    {
        ostr << (alter ? "ALTER QUOTA" : "CREATE QUOTA")
                     ;
    }

    if (if_exists)
        ostr << " IF EXISTS";
    else if (if_not_exists)
        ostr << " IF NOT EXISTS";
    else if (or_replace)
        ostr << " OR REPLACE";

    formatNames(names, ostr);

    if (!storage_name.empty())
        ostr
                    << " IN "
                    << backQuoteIfNeed(storage_name);

    formatOnCluster(ostr, settings);

    if (!new_name.empty())
        formatRenameTo(new_name, ostr, settings);

    if (key_type)
        formatKeyType(*key_type, ostr, settings);

    formatIntervalsWithLimits(all_limits, ostr, settings);

    if (roles && (!roles->empty() || alter))
        formatToRoles(*roles, ostr, settings);
}


void ASTCreateQuotaQuery::replaceCurrentUserTag(const String & current_user_name) const
{
    if (roles)
        roles->replaceCurrentUserTag(current_user_name);
}

}
