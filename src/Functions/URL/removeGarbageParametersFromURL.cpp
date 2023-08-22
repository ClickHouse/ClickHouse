#include <algorithm>
#include <vector>
#include <cmath>
#include <cstdlib>
#include <boost/math/distributions/chi_squared.hpp>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionRemoveGarbageParametersFromURL.h>
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
    /// This function implements Pearson's chi-squared test for uniform distribution. In simple words, it determines how significant are
    /// the differences between the distribution of symbols in string and the uniform distribution from character a to character b. If
    /// the differences are significant enough, the string is unlikely to be described by uniform distribution with given parameters.
    /// NOTE: This test sometimes give false positive results, especially on short (< 50 chars) strings.
    /// The results are not guaranteed to be accurate
    /// For more information visit https://en.wikipedia.org/wiki/Pearson%27s_chi-squared_test
    static bool pearsonIsUniform(const char * c_start, const char * c_end, char a, char b)
    {
        /// Deciding how much intervals to split symbols into and whether the string is long enough to be handled
        int64_t length = c_end - c_start;
        int l = static_cast<int>(3.32 * log10(length)) + 1;
        if (l < 2)
            return false;

        /// Creating points for start and end of intervals
        std::vector<double> alpha;
        alpha.resize(l);
        double change = static_cast<double>(b - a) / static_cast<double>(l - 1);
        for (int i = 0; i < l; ++i)
            alpha[i] = a + change * i;

        /// This is necessary in order lower_bound put last symbols of distribution to the last interval
        ++(alpha[l - 1]);

        /// Count the number of symbols that fit into each interval
        std::vector<int> n;
        n.resize(l - 1);
        for (const char * i = c_start; i < c_end; ++i)
        {
            std::vector<double>::iterator it = lower_bound(alpha.begin(), alpha.end(), *i);
            if (*it != *i || it == (alpha.end() - 1))
                --it;

            int64_t place = &(*it) - &(*alpha.begin());
            ++n[place];
        }

        /// Count the probability of getting into each interval for uniform distribution
        std::vector<double> p;
        p.resize(l - 1);
        for (int i = 0; i < l - 1; ++i)
            p[i] = (std::min(alpha[i + 1], static_cast<double>(b)) - alpha[i]) / (b - a);

        /// Estimate the number of characters that would fit into each interval
        std::vector<double> np;
        np.resize(l - 1);
        for (int i = 0; i < l - 1; ++i)
            np[i] = length * p[i];

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
            x += pow(n[i] - np[i], 2) / np[i];

        /// Calculating quantile
        double q;
        try
        {
            boost::math::chi_squared_distribution dist(static_cast<double>(l));
            q = quantile(dist, 0.95);
        }
        catch (std::domain_error &)
        {
            return false;
        }

        /// If Pearson statistics is from 0 to quantile, the distribution is uniform
        return 0 <= x && x <= q;
    }

    /// @brief Create a copyIntoOutput of string, move symbols to one interval and check if the distribution is uniform for base64 for URL string
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

    /// @brief Create a copyIntoOutput of string, move symbols to one interval and check if the distribution is uniform for hex string
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
                correction = -39;

            *write = *i + correction;
            ++write;
        }

        bool ret = pearsonIsUniform(n_c_start, n_c_end, '0', '?');
        free(n_c_start);
        return !ret;
    }

    /// @brief Check if string is long enough and if it could be hex or base64 for url
    /// @return if the parameter should be included in URL
    static bool process_content(const char * c_start, const char * c_end, uint64_t min_len)
    {
        if (static_cast<uint64_t>(c_end - c_start) < min_len)
            return true;

        bool check_hex = true;
        bool check_base64 = true;
        for (const char * i = c_start; i < c_end; ++i)
        {
            if (check_hex && !(('0' <= *i && *i <= '9') || ('a' <= *i && *i <= 'f')))
                check_hex = false;

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
            return true;

        return process_content(c_start, p_end, min_len);
    }

    /// @brief Add data to string
    /// @param from pointer to the first character of data
    /// @param from_length length of data to be added
    /// @param res_data String to add data to
    /// @param res_offsets offsets of output string
    /// @param data_pos Number of data piece
    static void copyIntoOutput(const char * from, size_t from_length, ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets, size_t data_pos)
    {
        size_t old_size = res_data.size();
        res_data.resize(old_size + from_length);
        memcpySmallAllowReadWriteOverflow15(&res_data[old_size], from, from_length);
        res_offsets[data_pos] += from_length;
    }

    /// @brief End the string with null terminator
    /// @param res_data String to finalize
    /// @param res_offsets Offsets of string
    /// @param data_pos Number of data piece
    static void finalizeData(ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets, size_t data_pos)
    {
        size_t old_size = res_data.size();
        res_data.resize(old_size + 1);
        res_data[old_size] = 0;
        ++res_offsets[data_pos];
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

        /// Iterate over data
        std::vector<const char *> p_start;
        std::vector<const char *> p_end;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            /// Prepare pointers
            size_t prev_offset;
            if (i > 0)
            {
                prev_offset = offsets[i - 1];
            } else
            {
                prev_offset = 0;
            }
            size_t cur_offset = offsets[i];
            const char * url_start = reinterpret_cast<const char *>(&data[prev_offset]);
            const char * url_end = reinterpret_cast<const char *>(&data[cur_offset]) - 1;
            size_t url_length = url_end - url_start;
            const char * cur = url_start;

            /// Set length
            res_offsets[i] = prev_offset;

            /// Calculate length of the protocol, domain and path
            int64_t addr_length = -1;
            for (size_t j = 0; j < url_length; ++j)
            {
                if (cur[j] == '?')
                {
                    addr_length = j;
                    break;
                }
                else if (cur[j] == 0)
                {
                    copyIntoOutput(url_start, url_length, res_data, res_offsets, i);
                    finalizeData(res_data, res_offsets, i);
                    return;
                }
            }
            if (addr_length == -1)
            {
                copyIntoOutput(url_start, url_length, res_data, res_offsets, i);
                finalizeData(res_data, res_offsets, i);
                return;
            }
            copyIntoOutput(url_start, addr_length, res_data, res_offsets, i);
            cur += addr_length + 1;

            /// Find the first and the last characters of parameters
            int64_t params_size = url_length - addr_length - 1;
            const char * params_start = cur;
            const char * params_end = cur + params_size;
            const char * cur_start = params_start;
            int params_number = 0;
            for (const char * j = params_start; j < params_end; ++j)
            {
                if (*j == '&')
                {
                    p_start.push_back(cur_start);
                    p_end.push_back(j);
                    cur_start = j + 1;
                    ++params_number;
                }
            }
            p_start.push_back(cur_start);
            p_end.push_back(params_end);
            ++params_number;

            /// Process each parameter and copy if necessary
            bool first = true;
            for (int j = 0; j < params_number; ++j)
            {
                bool in = process_param(p_start[j], p_end[j], min_len);
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
                    copyIntoOutput(&sep, 1, res_data, res_offsets, i);
                    copyIntoOutput(p_start[j], p_end[j] - p_start[j], res_data, res_offsets, i);
                }
            }

            /// Finalize output string
            finalizeData(res_data, res_offsets, i);
            p_start.clear();
            p_end.clear();
        }
    }
};

struct NameRemoveGarbageParametersFromURL
{
    static constexpr auto name = "removeGarbageParametersFromURL";
};
using FunctionRemoveGarbageParametersFromURLMeta
    = FunctionRemoveGarbageParametersFromURL<RemoveGarbageParametersFromURLImpl, NameRemoveGarbageParametersFromURL>;

REGISTER_FUNCTION(RemoveGarbageParametersFromURL)
{
    factory.registerFunction<FunctionRemoveGarbageParametersFromURLMeta>(
        {
            R"(
Removes parameters that look random from URL

Takes a string containing an URL and an unsigned integer representing the minimal length of parameters to be considered for removal

It returns a string containing an URL with randomly generated parameters removed.

Here is an example:
[example:all]

If you specify minimum length that is greater than zero, it may help not to remove short parameters with low accuracy of detection
[example:min_len]

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
