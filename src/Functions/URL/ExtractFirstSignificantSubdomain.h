#pragma once

#include <base/find_symbols.h>
#include "domain.h"
#include "tldLookup.h"
#include <Common/TLDListsHolder.h> /// TLDType

namespace DB
{

struct FirstSignificantSubdomainDefaultLookup
{
    bool operator()(StringRef host) const
    {
        return tldLookup::isValid(host.data, host.size);
    }
};

template <bool without_www>
struct ExtractFirstSignificantSubdomain
{
    static size_t getReserveLengthForElement() { return 10; }

    static void execute(const Pos data, const size_t size, Pos & res_data, size_t & res_size, Pos * out_domain_end = nullptr)
    {
        FirstSignificantSubdomainDefaultLookup loookup;
        return execute(loookup, data, size, res_data, res_size, out_domain_end);
    }

    template <class Lookup>
    static void execute(const Lookup & lookup, const Pos data, const size_t size, Pos & res_data, size_t & res_size, Pos * out_domain_end = nullptr)
    {
        res_data = data;
        res_size = 0;

        Pos tmp;
        size_t domain_length;
        ExtractDomain<without_www>::execute(data, size, tmp, domain_length);

        if (domain_length == 0)
            return;

        if (out_domain_end)
            *out_domain_end = tmp + domain_length;

        /// cut useless dot
        if (tmp[domain_length - 1] == '.')
            --domain_length;

        res_data = tmp;
        res_size = domain_length;

        const auto * begin = tmp;
        const auto * end = begin + domain_length;
        std::array<const char *, 3> last_periods{};

        const auto * pos = find_first_symbols<'.'>(begin, end);
        while (pos < end)
        {
            last_periods[2] = last_periods[1];
            last_periods[1] = last_periods[0];
            last_periods[0] = pos;
            pos = find_first_symbols<'.'>(pos + 1, end);
        }

        if (!last_periods[0])
            return;

        if (!last_periods[1])
        {
            res_size = last_periods[0] - begin;
            return;
        }

        if (!last_periods[2])
            last_periods[2] = begin - 1;

        const auto * end_of_level_domain = find_first_symbols<'/'>(last_periods[0], end);
        if (!end_of_level_domain)
        {
            end_of_level_domain = end;
        }

        size_t host_len = static_cast<size_t>(end_of_level_domain - last_periods[1] - 1);
        StringRef host{last_periods[1] + 1, host_len};
        if (lookup(host))
        {
            res_data += last_periods[2] + 1 - begin;
            res_size = last_periods[1] - last_periods[2] - 1;
        }
        else
        {
            res_data += last_periods[1] + 1 - begin;
            res_size = last_periods[0] - last_periods[1] - 1;
        }
    }

    /// The difference with execute() is due to custom TLD list can have records of any level,
    /// not only 2-nd level (like non-custom variant), so it requires more lookups.
    template <class Lookup>
    static void executeCustom(const Lookup & lookup, const Pos data, const size_t size, Pos & res_data, size_t & res_size, Pos * out_domain_end = nullptr)
    {
        res_data = data;
        res_size = 0;

        Pos tmp;
        size_t domain_length;
        ExtractDomain<without_www>::execute(data, size, tmp, domain_length);

        if (domain_length == 0)
            return;

        if (out_domain_end)
            *out_domain_end = tmp + domain_length;

        /// cut useless dot
        if (tmp[domain_length - 1] == '.')
            --domain_length;

        res_data = tmp;
        res_size = domain_length;

        const auto * begin = tmp;
        const auto * end = begin + domain_length;
        std::array<const char *, 2> last_periods{};
        last_periods[0] = begin - 1;
        StringRef excluded_host{};

        const auto * pos = find_first_symbols<'.'>(begin, end);
        while (pos < end)
        {
            size_t host_len = static_cast<size_t>(end - pos - 1);
            StringRef host{pos + 1, host_len};
            TLDType tld_type = lookup(host);
            switch (tld_type)
            {
                case TLDType::TLD_NONE:
                    break;
                case TLDType::TLD_REGULAR:
                    res_data += last_periods[0] + 1 - begin;
                    res_size = end - 1 - last_periods[0];
                    return;
                case TLDType::TLD_ANY:
                {
                    StringRef regular_host{last_periods[0] + 1, static_cast<size_t>(end - 1 - last_periods[0])};
                    if (last_periods[1] && excluded_host != regular_host)
                    {
                        /// Return TLD_REGULAR + 1
                        res_data += last_periods[1] + 1 - begin;
                        res_size = end - 1 - last_periods[1];
                    }
                    else
                    {
                        /// Same as TLD_REGULAR
                        res_data += last_periods[0] + 1 - begin;
                        res_size = end - 1 - last_periods[0];
                    }
                    return;
                }
                case TLDType::TLD_EXCLUDE:
                    excluded_host = host;
                    break;
            }

            last_periods[1] = last_periods[0];
            last_periods[0] = pos;
            pos = find_first_symbols<'.'>(pos + 1, end);
        }

        /// - if there is domain of the first level (i.e. no dots in the hostname) ->
        ///   return nothing
        if (last_periods[0] == begin - 1)
            return;

        /// - if there is domain of the second level ->
        ///   always return itself
        ///
        /// - if there is domain of the 3+ level, and zero records in TLD list ->
        ///   fallback to domain of the second level
        res_data += last_periods[1] + 1 - begin;
        res_size = last_periods[0] - last_periods[1] - 1;
    }
};

}

