#include <Parsers/Access/ASTCreateQuotaQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Common/quoteString.h>
#include <Common/IntervalKind.h>
#include <Common/SipHash.h>
#include <base/range.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{
namespace
{
    void formatIpPrefixBits(const std::optional<MaskBits> & ipv4_prefix_bits,
                            const std::optional<MaskBits> & ipv6_prefix_bits, WriteBuffer & ostr)
    {
        if (ipv4_prefix_bits)
            ostr << " IPV4_PREFIX_BITS " << static_cast<UInt64>(*ipv4_prefix_bits);
        if (ipv6_prefix_bits)
            ostr << " IPV6_PREFIX_BITS " << static_cast<UInt64>(*ipv6_prefix_bits);
    }

    void formatKeyType(const QuotaKeyType & key_type, const std::optional<MaskBits> & ipv4_prefix_bits,
                       const std::optional<MaskBits> & ipv6_prefix_bits, WriteBuffer & ostr, const IAST::FormatSettings &)
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

        if (key_type == QuotaKeyType::IP_ADDRESS || key_type == QuotaKeyType::FORWARDED_IP_ADDRESS)
            formatIpPrefixBits(ipv4_prefix_bits, ipv6_prefix_bits, ostr);
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
        ostr << " RENAME TO " << backQuoteIfNeed(new_name);
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
    auto res = make_intrusive<ASTCreateQuotaQuery>(*this);

    if (roles)
        res->roles = boost::static_pointer_cast<ASTRolesOrUsersSet>(roles->clone());

    return res;
}


void ASTCreateQuotaQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// `getID` is constant and `children` is empty for this query, so every `CREATE`/`ALTER QUOTA`
    /// collides in the base tree hash. The rewrite-rule matcher uses the tree hash for semantic
    /// equality, so fold every semantic field the formatter emits (and only those, so the hash
    /// survives the debug-build format -> parse -> format consistency check).
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);

    hash_state.update(alter);
    hash_state.update(attach);
    hash_state.update(if_exists);
    hash_state.update(if_not_exists);
    hash_state.update(or_replace);
    hash_state.update(cluster);
    hash_state.update(storage_name);
    hash_state.update(new_name);

    hash_state.update(names.size());
    for (const auto & name : names)
        hash_state.update(name);

    /// `key_type`, the prefix bits and `all_limits` are non-AST members emitted through several
    /// conditional branches (e.g. the prefix bits appear only for an IP key type). Fold exactly
    /// the text the formatter would emit for them, reusing the same helpers, so the hash both
    /// distinguishes different quotas and stays stable across the format -> parse round-trip.
    {
        const IAST::FormatSettings format_settings(/*one_line=*/ true);
        WriteBufferFromOwnString buf;
        if (key_type)
            formatKeyType(*key_type, ipv4_prefix_bits, ipv6_prefix_bits, buf, format_settings);
        else if (ipv4_prefix_bits || ipv6_prefix_bits)
            formatIpPrefixBits(ipv4_prefix_bits, ipv6_prefix_bits, buf);
        formatIntervalsWithLimits(all_limits, buf, format_settings);
        hash_state.update(buf.str());
    }

    /// The formatter emits `roles` only when it is non-empty or this is an `ALTER` (an empty `TO`
    /// on a `CREATE` is dropped), so collapse "present but empty and not alter" to the same hash
    /// as absent to match the round-trip.
    const bool roles_emitted = roles && (!roles->empty() || alter);
    hash_state.update(roles_emitted);
    if (roles_emitted)
        roles->updateTreeHash(hash_state, ignore_aliases);
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
        formatKeyType(*key_type, ipv4_prefix_bits, ipv6_prefix_bits, ostr, settings);
    else if (ipv4_prefix_bits || ipv6_prefix_bits)
    {
        /// `ALTER QUOTA q IPV4_PREFIX_BITS 16` does not include `KEYED BY`, so
        /// `key_type` is unset. We still need to format the prefix bits so that
        /// `ON CLUSTER` distribution (which serializes via `formatWithSecretsOneLine`)
        /// carries the option to replicas.
        formatIpPrefixBits(ipv4_prefix_bits, ipv6_prefix_bits, ostr);
    }

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
