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
    void formatKeyType(const QuotaKeyType & key_type, const IAST::FormattingBuffer & out)
    {
        const auto & type_info = QuotaKeyTypeInfo::get(key_type);
        if (key_type == QuotaKeyType::NONE)
        {
            out.writeKeyword(" NOT KEYED");
            return;
        }

        out.writeKeyword(" KEYED BY ");

        if (!type_info.base_types.empty())
        {
            bool need_comma = false;
            for (const auto & base_type : type_info.base_types)
            {
                if (std::exchange(need_comma, true))
                    out.ostr << ", ";
                out.ostr << QuotaKeyTypeInfo::get(base_type).name;
            }
            return;
        }

        out.ostr << type_info.name;
    }


    void formatNames(const Strings & names, const IAST::FormattingBuffer & out)
    {
        out.ostr << " ";
        bool need_comma = false;
        for (const String & name : names)
        {
            if (std::exchange(need_comma, true))
                out.ostr << ", ";
            out.ostr << backQuoteIfNeed(name);
        }
    }


    void formatRenameTo(const String & new_name, const IAST::FormattingBuffer & out)
    {
        out.writeKeyword(" RENAME TO ");
        out.ostr << backQuote(new_name);
    }


    void formatLimit(QuotaType quota_type, QuotaValue max_value, const IAST::FormattingBuffer & out)
    {
        const auto & type_info = QuotaTypeInfo::get(quota_type);
        out.ostr << " " << type_info.name << " = " << type_info.valueToString(max_value);
    }


    void formatIntervalWithLimits(const ASTCreateQuotaQuery::Limits & limits, const IAST::FormattingBuffer & out)
    {
        auto interval_kind = IntervalKind::fromAvgSeconds(limits.duration.count());
        Int64 num_intervals = limits.duration.count() / interval_kind.toAvgSeconds();

        out.writeKeyword(" FOR");
        out.writeKeyword(limits.randomize_interval ? " RANDOMIZED" : "");
        out.writeKeyword(" INTERVAL");
        out.ostr << " " << num_intervals << " ";
        out.writeKeyword(interval_kind.toLowercasedKeyword());

        if (limits.drop)
        {
            out.writeKeyword(" NO LIMITS");
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
                out.writeKeyword(" MAX");
                bool need_comma = false;
                for (auto quota_type : collections::range(QuotaType::MAX))
                {
                    auto quota_type_i = static_cast<size_t>(quota_type);
                    if (limits.max[quota_type_i])
                    {
                        if (std::exchange(need_comma, true))
                            out.ostr << ",";
                        formatLimit(quota_type, *limits.max[quota_type_i], out);
                    }
                }
            }
            else
                out.writeKeyword(" TRACKING ONLY");
        }
    }

    void formatIntervalsWithLimits(const std::vector<ASTCreateQuotaQuery::Limits> & all_limits, const IAST::FormattingBuffer & out)
    {
        bool need_comma = false;
        for (const auto & limits : all_limits)
        {
            if (need_comma)
                out.ostr << ",";
            need_comma = true;

            formatIntervalWithLimits(limits, out);
        }
    }

    void formatToRoles(const ASTRolesOrUsersSet & roles, const IAST::FormattingBuffer & out)
    {
        out.writeKeyword(" TO ");
        roles.format(out);
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


void ASTCreateQuotaQuery::formatImpl(const FormattingBuffer & out) const
{
    if (attach)
    {
        out.writeKeyword("ATTACH QUOTA");
    }
    else
    {
        out.writeKeyword(alter ? "ALTER QUOTA" : "CREATE QUOTA");
    }

    if (if_exists)
        out.writeKeyword(" IF EXISTS");
    else if (if_not_exists)
        out.writeKeyword(" IF NOT EXISTS");
    else if (or_replace)
        out.writeKeyword(" OR REPLACE");

    formatNames(names, out.copy());
    formatOnCluster(out.copy());

    if (!new_name.empty())
        formatRenameTo(new_name, out.copy());

    if (key_type)
        formatKeyType(*key_type, out.copy());

    formatIntervalsWithLimits(all_limits, out.copy());

    if (roles && (!roles->empty() || alter))
        formatToRoles(*roles, out.copy());
}


void ASTCreateQuotaQuery::replaceCurrentUserTag(const String & current_user_name) const
{
    if (roles)
        roles->replaceCurrentUserTag(current_user_name);
}

}
