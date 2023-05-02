#include <algorithm>
#include <vector>
#include <malloc.h>
#include <math.h>
#include <string.h>
#include <boost/math/distributions/chi_squared.hpp>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringUIntToString.h>
#include <base/find_symbols.h>
#include "protocol.h"

namespace DB
{

struct RemoveGarbageParametersFromURLImpl
{
    /// @brief Guesses with 95% accuracy whether symbols in string form uniform distribution
    /// @param c_start Pointer to the first symbol of the string
    /// @param c_end Pointer to the symbol following the last symbol of the string
    /// @param a First character of uniform distribution
    /// @param b Last character of uniform distribution
    /// @return true - if there is 95%+ probability the string matches uniform distribution with given parameters, false - otherwise
    static bool pearsonIsUniform(const char * c_start, const char * c_end, char a, char b)
    {
        /// Deciding how much intervals to split symbols into and whether the string is long enough to be handled
        long length = c_end - c_start;
        int l = static_cast<int>(3.32 * log10(length)) + 1;
        if (l < 3)
        {
            return false;
        }

        /// Creating points for start and end of intervals
        std::vector<double> alpha;
        alpha.resize(l);
        double change = static_cast<double>(b - a) / static_cast<double>(l - 1);
        for (int i = 0; i < l; ++i)
        {
            alpha[i] = a + change * i;
        }
        /// This is necessary in order std::lower_bound put last symbols of distribution to the last interval
        ++(alpha[l - 1]);

        /// Count the number of symbols that fit into each interval
        std::vector<int> n;
        n.resize(l - 1);
        for (const char * i = c_start; i < c_end; ++i)
        {
            std::vector<double>::iterator it = std::lower_bound(alpha.begin(), alpha.end(), *i);
            if (*it != *i || it == (alpha.end() - 1))
            {
                --it;
            }
            long place = &(*it) - &(*alpha.begin());
            ++n[place];
        }

        /// Count the probability of getting into each interval for uniform distribution
        std::vector<double> p;
        p.resize(l - 1);
        for (int i = 0; i < l - 1; ++i)
        {
            p[i] = (std::min(alpha[i + 1], static_cast<double>(b)) - alpha[i]) / (b - a);
        }

        /// Estimate the number of characters that would fit into each interval
        std::vector<double> np;
        np.resize(l - 1);
        for (int i = 0; i < l - 1; ++i)
        {
            np[i] = length * p[i];
        }

        /// If less than 5 characters fit into an interval, merge it with the next one
        for (int i = 0; i < l - 2; ++i)
        {
            if (np[i] < 5)
            {
                alpha.erase(alpha.begin() + i + 1);
                n[i] += n[i + 1];
                n.erase(n.begin() + i + 1);
                p[i] += p[i + 1];
                p.erase(p.begin() + i + 1);
                np[i] += np[i + 1];
                np.erase(np.begin() + i + 1);
                --i;
                --l;
            }
        }

        /// ...or with the previous one
        for (int i = 1; i < l - 1; ++i)
        {
            if (np[i] < 5)
            {
                alpha.erase(alpha.begin() + i);
                n[i] += n[i - 1];
                n.erase(n.begin() + i - 1);
                p[i] += p[i - 1];
                p.erase(p.begin() + i - 1);
                np[i] += np[i - 1];
                np.erase(np.begin() + i - 1);
                --i;
                --l;
            }
        }

        /// Calculation Person statistics
        double x = 0;
        for (int i = 0; i < l - 1; ++i)
        {
            x += pow(n[i] - np[i], 2) / np[i];
        }

        /// Calculating quantile
        double q;
        try
        {
            boost::math::chi_squared_distribution dist(static_cast<double>(l - 1));
            q = quantile(dist, 0.95);
        }
        catch (std::domain_error &)
        {
            return false;
        }

        /// If Pearson statistics is from 0 to quantile, the distribution is uniform
        return 0 <= x && x <= q;
    }

    /// @brief Create a copy of string, move symbols to one interval and check if the distribution is uniform for base64 for URL string
    /// @param c_start Pointer to the first symbol of the string
    /// @param c_end Pointer to the symbol following the last symbol of the string
    /// @return if the parameter should be included in URL
    static bool checkBase64(const char * c_start, const char * c_end)
    {
        char * n_c_start = reinterpret_cast<char *>(malloc(c_end - c_start));
        const char * n_c_end = n_c_start - c_start + c_end;
        char * write = n_c_start;

        for (const char * i = c_start; i < c_end; ++i)
        {
            char correction = 0;
            if ('a' <= *i && *i <= 'z')
            {
                correction = -6;
            }
            else if ('0' <= *i && *i <= '9')
            {
                correction = 7;
            }
            else if ('-' == *i)
            {
                correction = 72;
            }
            else if ('_' == *i)
            {
                correction = 23;
            }
            else if ('=' == *i)
            {
                correction = 58;
            }
            *write = *i + correction;
            ++write;
        }

        bool ret = pearsonIsUniform(n_c_start, n_c_end, '7', 'w');
        free(n_c_start);
        return !ret;
    }

    /// @brief Create a copy of string, move symbols to one interval and check if the distribution is uniform for hex string
    /// @param c_start Pointer to the first symbol of the string
    /// @param c_end Pointer to the symbol following the last symbol of the string
    /// @return if the parameter should be included in URL
    static bool checkHex(const char * c_start, const char * c_end)
    {
        char * n_c_start = reinterpret_cast<char *>(malloc(c_end - c_start));
        const char * n_c_end = n_c_start - c_start + c_end;
        char * write = n_c_start;

        for (const char * i = c_start; i < c_end; ++i)
        {
            char correction = 0;
            if ('a' <= *i && *i <= 'f')
            {
                correction = -39;
            }
            *write = *i + correction;
            ++write;
        }

        bool ret = pearsonIsUniform(n_c_start, n_c_end, '0', '?');
        free(n_c_start);
        return !ret;
    }

    /// @brief Check if string is long enough and if it could be hex or base64 for url
    /// @param c_start Pointer to the first symbol of the string
    /// @param c_end Pointer to the symbol following the last symbol of the string
    /// @param min_len minimum string length
    /// @return if the parameter should be included in URL
    static bool process_content(const char * c_start, const char * c_end, uint64_t min_len)
    {
        if (static_cast<uint64_t>(c_end - c_start) < min_len)
        {
            return true;
        }

        bool check_hex = true;
        bool check_base64 = true;
        for (const char * i = c_start; i < c_end; ++i)
        {
            if (check_hex && !(('0' <= *i && *i <= '9') || ('a' <= *i && *i <= 'f')))
            {
                check_hex = false;
            }
            if (check_base64
                && !(
                    ('A' <= *i && *i <= 'Z') || ('a' <= *i && *i <= 'z') || ('0' <= *i && *i <= '9') || (*i == '-') || (*i == '_')
                    || (*i == '=')))
            {
                check_base64 = false;
                break;
            }
        }
        if (!check_hex && !check_base64)
        {
            return true;
        }
        else if (check_hex && check_base64)
        {
            return checkHex(c_start, c_end) && checkBase64(c_start, c_end);
        }
        else
        {
            return checkBase64(c_start, c_end);
        }
    }

    /// @brief Select the value of parameter and check if it is randomly generated
    /// @param p_start Pointer to the first symbol of the parameter
    /// @param p_end Pointer to the symbol following the last symbol of the parameter
    /// @param min_len minimum length of parameter's value
    /// @return if the parameter should be included in URL
    static bool process_param(const char * p_start, const char * p_end, uint64_t min_len)
    {
        const char * c_start = nullptr;
        for (const char * i = p_start; i < p_end; ++i)
        {
            if (*i == '=')
            {
                c_start = i + 1;
                break;
            }
        }
        if (c_start == nullptr)
        {
            return true;
        }
        return process_content(c_start, p_end, min_len);
    }

    /// @brief Add data to string
    /// @param res_data String to add data to
    /// @param size length of data to be added
    /// @param pdata pointer to the first character of data
    /// @param res_offsets offsets of output string
    static void copy(ColumnString::Chars & res_data, size_t size, const char * pdata, ColumnString::Offsets & res_offsets)
    {
        size_t old_size = res_data.size();
        res_data.resize(old_size + size);
        memcpySmallAllowReadWriteOverflow15(&res_data[old_size], pdata, size);
        res_offsets[0] += size;
    }

    /// @brief End the string with null terminator
    /// @param res_data String to finalize
    /// @param res_offsets Offsets of string
    static void finalize_data(ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
    {
        size_t old_size = res_data.size();
        res_data.resize(old_size + 1);
        res_data[old_size] = 0;
        ++res_offsets[0];
    }

    /// @brief Remove base64 for URL and hex parameters that look random from URL
    /// @param data URL
    /// @param offsets URL offsets
    /// @param min_len minimum length of parameter's value to handle
    /// @param res_data output string (should exist)
    /// @param res_offsets output string offsets (should exist)
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        uint64_t min_len,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        /// Reserve memory
        res_data.reserve(data.size());
        res_offsets.resize(offsets.size());

        /// Set length
        res_offsets[0] = 0;

        /// Prepare pointers
        size_t prev_offset = 0;
        size_t cur_offset = offsets[0];
        const char * pdata = reinterpret_cast<const char *>(&data[prev_offset]);
        const char * pdata_end = reinterpret_cast<const char *>(&data[cur_offset]) - 1;
        size_t size = pdata_end - pdata;
        const char * read = pdata;

        /// Calculate length of webpage address and copy it
        long url_size = -1;
        for (size_t i = 0; i < size; ++i)
        {
            if (read[i] == '?')
            {
                url_size = i;
                break;
            }
            else if (read[i] == 0)
            {
                copy(res_data, size, pdata, res_offsets);
                finalize_data(res_data, res_offsets);
                return;
            }
        }
        if (url_size == -1)
        {
            copy(res_data, size, pdata, res_offsets);
            finalize_data(res_data, res_offsets);
            return;
        }
        copy(res_data, url_size, pdata, res_offsets);
        read += url_size + 1;

        /// Find the first and the last characters of parameters
        long params_size = size - url_size - 1;
        const char * params_start = read;
        const char * params_end = read + params_size;
        std::vector<const char *> p_start;
        std::vector<const char *> p_end;
        const char * cur_start = params_start;
        int params_number = 0;
        for (const char * i = params_start; i < params_end; ++i)
        {
            if (*i == '&')
            {
                p_start.push_back(cur_start);
                p_end.push_back(i);
                cur_start = i + 1;
                ++params_number;
            }
        }
        p_start.push_back(cur_start);
        p_end.push_back(params_end);
        ++params_number;

        /// Process each parameter and copy if necessary
        bool first = true;
        for (int i = 0; i < params_number; ++i)
        {
            bool in = process_param(p_start[i], p_end[i], min_len);
            if (in)
            {
                char sep;
                if (first)
                {
                    sep = '?';
                    first = false;
                }
                else
                {
                    sep = '&';
                }
                copy(res_data, 1, &sep, res_offsets);
                copy(res_data, p_end[i] - p_start[i], p_start[i], res_offsets);
            }
        }

        /// Finalize output string
        finalize_data(res_data, res_offsets);
    }
};

struct NameRemoveGarbageParametersFromURL
{
    static constexpr auto name = "removeGarbageParametersFromURL";
};
using FunctionRemoveGarbageParametersFromURL
    = FunctionsStringUIntToString<RemoveGarbageParametersFromURLImpl, NameRemoveGarbageParametersFromURL>;

REGISTER_FUNCTION(RemoveGarbageParametersFromURL)
{
    factory.registerFunction<FunctionRemoveGarbageParametersFromURL>(
        {
            R"(
Removes parameters that look random from URL

Takes a string containing an URL and an unsigned integer representing the minimal length of parameters to be considered for removal

It returns a string containing an URL with randomly generated parameters removed.

Here is an example:
[example:all]

If you specify minimum length that is greater than zero, it may help not to remove short parameters with low accuracy of detection
[example:min_len]

Every self-respectful data scientist knows how to apply arcsine to improve ads click-through rate with ClickHouse.
For more details, see [https://en.wikipedia.org/wiki/Inverse_trigonometric_functions].
)",
            Documentation::Examples{
                {"all", "SELECT removeGarbageParametersFromURL('http://yandex.ru/clck/jsredir?from=yandex.ru;search%2F;web;;&text=&etext=1004&cst=AiuY0DBWFJ5Hyx_fyvalFPA3abBqdnSOApSiLPWwVkIeiz46AroRCQXrfJ8M5oYTWorWEWccK4Kw_QEhSDD6X4nGMT4OabEk0xnry4NtnOEzWFPU4iTzunSVVjkuY7CYolnD7hb04cMRv7iMnaO8LjNm0hxqvwN9sXCzUYeXp_muLsdY4W99_U5MJKGmz7IAmR5-ceoAoaBB2XGYAS9BTYKbbvlmneBpbf_SwAd_6OOACXtLmRXqXad3AQbcArYE8LCO0zmE9vpha3yoT0jl8pd9CUmbGZR5nA3sf5TcDFTpr5nYaOdxjmHep2cZeW3QHvPtKA2xWXW6qzGrQeZ1SEOPcJ1afJqmAHisup90hhNYyl2hxl8xn_DtCRbJqYHb88JtuQ3591EGW42wPZhSbxBFdU0KIZN3c_VZOmk6avzKzqG_kJpjPObWXbh9qs0S23WxDGCcPUrIzi3ESSLv1qgaRhqkfjBc57BFVA4RxlljpKQdeVeTbklJgqptznf1aHZQ2wYARBzC_jvv994MCTZIus_NctCMWoSaU74OaMmo0h5ScYLI2CWy6nj5PbhCrgeLsaEBVOQT9xoLSoCRfJ78xI_T1ruuD3QBJmHY6YW8f5UM36LRbzhd5vmNTPRvrs2wcCFhF_w&l10n=ru&cts=1458904227333&mc=1.584962500721156', 0)"},
                {"min_len", "SELECT removeGarbageParametersFromURL('http://yandex.ru/clck/jsredir?from=yandex.ru;search%2F;web;;&text=&etext=1004&cst=AiuY0DBWFJ5Hyx_fyvalFPA3abBqdnSOApSiLPWwVkIeiz46AroRCQXrfJ8M5oYTWorWEWccK4Kw_QEhSDD6X4nGMT4OabEk0xnry4NtnOEzWFPU4iTzunSVVjkuY7CYolnD7hb04cMRv7iMnaO8LjNm0hxqvwN9sXCzUYeXp_muLsdY4W99_U5MJKGmz7IAmR5-ceoAoaBB2XGYAS9BTYKbbvlmneBpbf_SwAd_6OOACXtLmRXqXad3AQbcArYE8LCO0zmE9vpha3yoT0jl8pd9CUmbGZR5nA3sf5TcDFTpr5nYaOdxjmHep2cZeW3QHvPtKA2xWXW6qzGrQeZ1SEOPcJ1afJqmAHisup90hhNYyl2hxl8xn_DtCRbJqYHb88JtuQ3591EGW42wPZhSbxBFdU0KIZN3c_VZOmk6avzKzqG_kJpjPObWXbh9qs0S23WxDGCcPUrIzi3ESSLv1qgaRhqkfjBc57BFVA4RxlljpKQdeVeTbklJgqptznf1aHZQ2wYARBzC_jvv994MCTZIus_NctCMWoSaU74OaMmo0h5ScYLI2CWy6nj5PbhCrgeLsaEBVOQT9xoLSoCRfJ78xI_T1ruuD3QBJmHY6YW8f5UM36LRbzhd5vmNTPRvrs2wcCFhF_w&l10n=ru&cts=1458904227333&mc=1.584962500721156', 14)"}},
            Documentation::Categories{"URLs", "Strings", "Searching in Strings"}
        },
        FunctionFactory::CaseInsensitive
    );
}

}
