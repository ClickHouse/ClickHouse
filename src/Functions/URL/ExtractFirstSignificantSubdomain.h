#pragma once

#include <common/find_symbols.h>
#include "domain.h"
#include "tldLookup.h"

namespace DB
{

struct FirstSignificantSubdomainDefaultLookup
{
    bool operator()(const char *src, size_t len) const
    {
        return tldLookup::isValid(src, len);
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

        auto begin = tmp;
        auto end = begin + domain_length;
        const char * last_3_periods[3]{};

        auto pos = find_first_symbols<'.'>(begin, end);
        while (pos < end)
        {
            last_3_periods[2] = last_3_periods[1];
            last_3_periods[1] = last_3_periods[0];
            last_3_periods[0] = pos;
            pos = find_first_symbols<'.'>(pos + 1, end);
        }

        if (!last_3_periods[0])
            return;

        if (!last_3_periods[1])
        {
            res_size = last_3_periods[0] - begin;
            return;
        }

        if (!last_3_periods[2])
            last_3_periods[2] = begin - 1;

        auto end_of_level_domain = find_first_symbols<'/'>(last_3_periods[0], end);
        if (!end_of_level_domain)
        {
            end_of_level_domain = end;
        }

        if (lookup(last_3_periods[1] + 1, end_of_level_domain - last_3_periods[1] - 1))
        {
            res_data += last_3_periods[2] + 1 - begin;
            res_size = last_3_periods[1] - last_3_periods[2] - 1;
        }
        else
        {
            res_data += last_3_periods[1] + 1 - begin;
            res_size = last_3_periods[0] - last_3_periods[1] - 1;
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

        auto begin = tmp;
        auto end = begin + domain_length;
        const char * last_2_periods[2]{};
        const char * prev = begin - 1;

        auto pos = find_first_symbols<'.'>(begin, end);
        while (pos < end)
        {
            if (lookup(pos + 1, end - pos - 1))
            {
                res_data += prev + 1 - begin;
                res_size = end - 1 - prev;
                return;
            }

            last_2_periods[1] = last_2_periods[0];
            last_2_periods[0] = pos;
            prev = pos;
            pos = find_first_symbols<'.'>(pos + 1, end);
        }

        /// if there is domain of the first level (i.e. no dots in the hostname) -> return nothing
        if (!last_2_periods[0])
            return;

        /// if there is domain of the second level -> always return itself
        if (!last_2_periods[1])
        {
            res_size = last_2_periods[0] - begin;
            return;
        }

        /// if there is domain of the 3+ level, and zero records in TLD list ->
        /// fallback to domain of the second level
        res_data += last_2_periods[1] + 1 - begin;
        res_size = last_2_periods[0] - last_2_periods[1] - 1;
    }
};

}

