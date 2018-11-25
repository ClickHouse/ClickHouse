#pragma once

#include <Functions/domain.h>
#include <common/find_symbols.h>


namespace DB
{

struct ExtractFirstSignificantSubdomain
{
    static size_t getReserveLengthForElement() { return 10; }

    static void execute(const Pos data, const size_t size, Pos & res_data, size_t & res_size, Pos * out_domain_end = nullptr)
    {
        res_data = data;
        res_size = 0;

        Pos tmp;
        size_t domain_length;
        ExtractDomain<true>::execute(data, size, tmp, domain_length);

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

        size_t size_of_second_subdomain_plus_period = last_3_periods[0] - last_3_periods[1];
        if (size_of_second_subdomain_plus_period == 4 || size_of_second_subdomain_plus_period == 3)
        {
            /// We will key by four bytes that are either ".xyz" or ".xy.".
            UInt32 key = unalignedLoad<UInt32>(last_3_periods[1]);

            /// NOTE: assuming little endian.
            /// NOTE: does the compiler generate SIMD code?
            /// NOTE: for larger amount of cases we can use a perfect hash table (see 'gperf' as an example).
            if (   key == '.' + 'c' * 0x100U + 'o' * 0x10000U + 'm' * 0x1000000U
                || key == '.' + 'n' * 0x100U + 'e' * 0x10000U + 't' * 0x1000000U
                || key == '.' + 'o' * 0x100U + 'r' * 0x10000U + 'g' * 0x1000000U
                || key == '.' + 'b' * 0x100U + 'i' * 0x10000U + 'z' * 0x1000000U
                || key == '.' + 'g' * 0x100U + 'o' * 0x10000U + 'v' * 0x1000000U
                || key == '.' + 'm' * 0x100U + 'i' * 0x10000U + 'l' * 0x1000000U
                || key == '.' + 'e' * 0x100U + 'd' * 0x10000U + 'u' * 0x1000000U
                || key == '.' + 'c' * 0x100U + 'o' * 0x10000U + '.' * 0x1000000U)
            {
                res_data += last_3_periods[2] + 1 - begin;
                res_size = last_3_periods[1] - last_3_periods[2] - 1;
                return;
            }
        }

        res_data += last_3_periods[1] + 1 - begin;
        res_size = last_3_periods[0] - last_3_periods[1] - 1;
    }
};

}

