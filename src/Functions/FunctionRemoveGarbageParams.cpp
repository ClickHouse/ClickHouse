#include <algorithm>
#include <iostream>
#include <iterator>
#include <string_view>
#include <vector>

bool checkingForNormality(const std::string_view & value);

struct UrlParam
{
    std::string_view key;
    std::string_view value;
};

std::vector<UrlParam> getParamsFromUrl(const std::string & url)
{
    std::vector<UrlParam> result;

    const auto start_params = std::find(url.begin(), url.end(), '?');

    auto start_current_param = start_params;
    while (start_current_param != url.end())
    {
        const auto start_current_param_key = start_current_param + 1;
        const auto end_current_param_key = std::find(start_current_param_key, url.end(), '=');

        const auto start_current_param_value = end_current_param_key + 1;
        const auto end_current_param_value = std::find(start_current_param_value, url.end(), '&');

        result.push_back({
            .key{start_current_param_key, end_current_param_key},
            .value{start_current_param_value, end_current_param_value},
        });

        start_current_param = end_current_param_value;
    }
    return result;
}

std::string normalizeParamsUrl(const std::string & url)
{
    const auto params = getParamsFromUrl(url);
    if (params.empty())
    {
        return url;
    }

    const auto start_params = std::find(url.begin(), url.end(), '?');
    std::string result(url.begin(), start_params);

    auto it = params.begin();
    while (it != params.end() && checkingForNormality(it->value))
    {
        ++it;
    }
    if (it != params.end())
    {
        result += '?' + std::string(it->key) + '=' + std::string(it->value);
        for (++it; it != params.end(); ++it)
        {
            if (!checkingForNormality(it->value))
            {
                result += '&' + std::string(it->key) + '=' + std::string(it->value);
            }
        }
    }
    return result;
}
