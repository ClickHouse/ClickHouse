// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <unordered_set>

#include <Interpreters/AI/AIRequestOptions.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/Exception.h>
#include <Poco/Util/AbstractConfiguration.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wcovered-switch-default"
#include <nlohmann/json.hpp>
#pragma clang diagnostic pop

#include "config.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

AIRequestOptions AIRequestOptions::fromJSON(const String & json_str)
{
    AIRequestOptions opts;
    if (json_str.empty())
        return opts;

    try
    {
        auto json = nlohmann::json::parse(json_str);

        if (!json.is_object())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Options argument must be a JSON object");

        /// Helper lambdas for extracting values
        auto get_string = [&](const std::string & key, std::optional<String> & target)
        {
            if (json.contains(key) && json[key].is_string())
                target = json[key].get<std::string>();
        };

        auto get_double = [&](const std::string & key, std::optional<double> & target)
        {
            if (json.contains(key) && json[key].is_number())
                target = json[key].get<double>();
        };

        auto get_int = [&](const std::string & key, std::optional<int> & target)
        {
            if (json.contains(key) && json[key].is_number_integer())
                target = json[key].get<int>();
        };

        auto get_size_t = [&](const std::string & key, size_t & target)
        {
            if (json.contains(key) && json[key].is_number_integer())
            {
                int64_t value = json[key].get<int64_t>();
                if (value < 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} must be >= 0", key);
                target = static_cast<size_t>(value);
            }
        };

        /// Extract all fields
        get_string("provider", opts.provider);
        get_string("model", opts.model);
        get_string("api_key", opts.api_key);
        get_string("base_url", opts.base_url);
        get_double("temperature", opts.temperature);
        get_int("max_tokens", opts.max_tokens);
        get_double("top_p", opts.top_p);
        get_int("seed", opts.seed);
        get_double("frequency_penalty", opts.frequency_penalty);
        get_double("presence_penalty", opts.presence_penalty);
        get_size_t("requests_per_minute", opts.requests_per_minute);
        get_size_t("max_retries", opts.max_retries);
        get_size_t("retry_delay_ms", opts.retry_delay_ms);
        get_size_t("retry_max_delay_ms", opts.retry_max_delay_ms);

        /// Check for unknown keys
        static const std::unordered_set<std::string> known_keys = {
            "provider", "model", "api_key", "base_url", "temperature", "max_tokens", "top_p", "seed",
            "frequency_penalty", "presence_penalty", "requests_per_minute",
            "max_retries", "retry_delay_ms", "retry_max_delay_ms"
        };

        for (auto it = json.begin(); it != json.end(); ++it)
        {
            if (known_keys.find(it.key()) == known_keys.end())
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Unknown option '{}' in options_json. Supported options: provider, model, api_key, base_url, temperature, "
                    "max_tokens, top_p, seed, frequency_penalty, presence_penalty, requests_per_minute, "
                    "max_retries, retry_delay_ms",
                    it.key());
            }
        }
    }
    catch (const nlohmann::json::parse_error & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid JSON in options argument: {}", e.what());
    }

    return opts;
}

void AIRequestOptions::mergeWithConfig(const Poco::Util::AbstractConfiguration & config)
{
    try
    {
        /// Helper lambda to merge optional values from config
        auto mergeOptionalString = [&](std::optional<String> & opt, const char * key)
        {
            if (!opt && config.has(key))
                opt = config.getString(key);
        };

        auto mergeOptionalDouble = [&](std::optional<double> & opt, const char * key)
        {
            if (!opt && config.has(key))
                opt = config.getDouble(key);
        };

        auto mergeOptionalInt = [&](std::optional<int> & opt, const char * key)
        {
            if (!opt && config.has(key))
                opt = config.getInt(key);
        };

        auto mergeDefaultSize = [&](size_t & value, size_t default_value, const char * key)
        {
            if (value == default_value && config.has(key))
                value = config.getUInt(key);
        };

        /// Merge string options
        mergeOptionalString(provider, "ai.provider");
        mergeOptionalString(model, "ai.model");
        mergeOptionalString(api_key, "ai.api_key");
        mergeOptionalString(base_url, "ai.base_url");

        /// Merge generation parameters
        mergeOptionalDouble(temperature, "ai.temperature");
        mergeOptionalInt(max_tokens, "ai.max_tokens");
        mergeOptionalDouble(top_p, "ai.top_p");
        mergeOptionalInt(seed, "ai.seed");
        mergeOptionalDouble(frequency_penalty, "ai.frequency_penalty");
        mergeOptionalDouble(presence_penalty, "ai.presence_penalty");

        /// Merge control parameters (with default values)
        mergeDefaultSize(requests_per_minute, 0, "ai.requests_per_minute");
        mergeDefaultSize(max_retries, 3, "ai.max_retries");
        mergeDefaultSize(retry_delay_ms, 1000, "ai.retry_delay_ms");
        mergeDefaultSize(retry_max_delay_ms, 60000, "ai.retry_max_delay_ms");
    }
    catch (const Poco::Exception & e)
    {
        /// Config value type mismatch (e.g. ai.temperature set to a non-numeric string) - log and use defaults
        LOG_WARNING(getLogger("AIRequestOptions"), "Failed to read AI settings from server config: {}. Check config.xml for type errors.", e.message());
    }
}

void AIRequestOptions::validate() const
{
    if (!provider || provider->empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "AI provider not specified. Set 'provider' in options_json or 'ai.provider' in config.xml");

    if (!model || model->empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "AI model not specified. Set 'model' in options_json or 'ai.model' in config.xml");

    if (max_retries > 20)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "max_retries must be <= 20, got {}", max_retries);

    /// Prevent overflow in exponential backoff: retry_delay_ms * (1 << max_retries) must fit in uint64_t.
    /// With max_retries <= 20, the shift is at most 2^19 = 524288.
    /// So retry_delay_ms must be <= UINT64_MAX / 524288 ~ 3.5e13, but we cap at 3600000 (1 hour) as a sanity check.
    if (retry_delay_ms > 3600000)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "retry_delay_ms must be <= 3600000 (1 hour), got {}", retry_delay_ms);

    if (retry_max_delay_ms > 0 && retry_max_delay_ms < retry_delay_ms)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "retry_max_delay_ms ({}) must be >= retry_delay_ms ({})", retry_max_delay_ms, retry_delay_ms);
}

}
