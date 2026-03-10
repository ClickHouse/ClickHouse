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

#include <Interpreters/AI/AITextGenerator.h>
#include <Interpreters/AI/AIPromptRender.h>
#include <Interpreters/Context.h>
#include <Client/AI/AIClientFactory.h>
#include <Client/AI/AIConfiguration.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>
#include <ai/types/generate_options.h>
#include <ai/types/client.h>
#include <ai/errors.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
}

AITextGenerator::AITextGenerator(ContextPtr context_)
    : context(context_)
{
}

ColumnPtr AITextGenerator::generateText(
    const String & system_prompt,
    const ColumnString & user_prompts,
    const AIRequestOptions & options,
    size_t input_rows_count) const
{
    /// Create AI client
    AIConfiguration config;
    config.provider = *options.provider;
    config.model = *options.model;

    /// Use api_key and base_url from options (already merged with config)
    if (options.api_key)
        config.api_key = *options.api_key;
    if (options.base_url)
        config.base_url = *options.base_url;

    auto result = AIClientFactory::createClient(config);
    if (!result.client.has_value())
        throw Exception(
            ErrorCodes::NETWORK_ERROR,
            "Failed to create AI client for provider '{}'. "
            "Set the API key via environment variable "
            "(OPENAI_API_KEY / ANTHROPIC_API_KEY) or server configuration",
            *options.provider);

    auto client = std::move(*result.client);

    /// Create rate limiter
    RequestRateLimiter rate_limiter(options.requests_per_minute);

    /// Process each row
    const auto & data = user_prompts.getChars();
    const auto & offsets = user_prompts.getOffsets();
    auto result_column = ColumnString::create();
    auto & res_data = result_column->getChars();
    auto & res_offsets = result_column->getOffsets();

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        rate_limiter.wait();

        /// Extract user prompt for this row
        size_t prev_offset = (i == 0) ? 0 : offsets[i - 1];
        String user_prompt(reinterpret_cast<const char *>(&data[prev_offset]), offsets[i] - prev_offset);

        /// Generate text
        String response = generateSingle(client, *options.model, system_prompt, user_prompt, options);

        /// Insert result
        size_t response_size = response.size();
        size_t old_size = res_data.size();
        res_data.resize(old_size + response_size + 1);
        memcpy(&res_data[old_size], response.data(), response_size);
        res_data[old_size + response_size] = 0;
        res_offsets.push_back(old_size + response_size + 1);
    }

    return result_column;
}

String AITextGenerator::generateSingle(
    ai::Client & client,
    const String & model,
    const String & system_prompt,
    const String & user_prompt,
    const AIRequestOptions & options) const
{
    /// Build prompt
    String prompt = AIPromptRender::buildPrompt(system_prompt, user_prompt);

    /// Retry loop
    for (size_t attempt = 0; attempt <= options.max_retries; ++attempt)
    {
        try
        {
            /// Create generate options
            ai::GenerateOptions generate_opts;
            generate_opts.model = model;
            generate_opts.prompt = prompt;

            /// Set system prompt (use default if not provided)
            if (!system_prompt.empty())
            {
                generate_opts.system = system_prompt;
            }
            else
            {
                generate_opts.system = AIPromptRender::getDefaultSystemPrompt();
            }

            /// Apply user options
            applyOptions(generate_opts, options);

            /// Call LLM
            ai::GenerateResult result = client.generate_text(generate_opts);

            if (!result.is_success())
                throw Exception(ErrorCodes::NETWORK_ERROR, "AI API returned error: {}", result.error_message());

            return result.text;
        }
        catch (const ai::AIError & e)
        {
            if (attempt < options.max_retries && isRetryableError(e.what()))
            {
                const uint64_t delay_ms = options.retry_delay_ms * (static_cast<size_t>(1) << attempt);
                LOG_WARNING(
                    getLogger("AITextGenerator"),
                    "Retryable error detected, retrying after {}ms (attempt {}/{}): {}",
                    delay_ms, attempt + 1, options.max_retries, e.what());
                sleepForMilliseconds(delay_ms);
                continue;
            }
            throw Exception(ErrorCodes::NETWORK_ERROR, "AI API call failed: {}", e.what());
        }
        catch (const std::exception & e)
        {
            if (attempt < options.max_retries && isRetryableError(e.what()))
            {
                const uint64_t delay_ms = options.retry_delay_ms * (static_cast<size_t>(1) << attempt);
                LOG_WARNING(
                    getLogger("AITextGenerator"),
                    "Retryable error detected, retrying after {}ms (attempt {}/{}): {}",
                    delay_ms, attempt + 1, options.max_retries, e.what());
                sleepForMilliseconds(delay_ms);
                continue;
            }
            throw Exception(ErrorCodes::NETWORK_ERROR, "AI API call failed: {}", e.what());
        }
    }

    /// Unreachable, but satisfies the compiler
    throw Exception(ErrorCodes::NETWORK_ERROR, "AI API call failed after {} retries", options.max_retries);
}

bool AITextGenerator::isRetryableError(const String & msg)
{
    /// Rate limit (429)
    if (msg.find("429") != String::npos)
        return true;

    /// Server errors (5xx)
    if (msg.find("500") != String::npos
        || msg.find("502") != String::npos
        || msg.find("503") != String::npos
        || msg.find("504") != String::npos)
        return true;

    return false;
}

void AITextGenerator::applyOptions(
    ai::GenerateOptions & generate_opts,
    const AIRequestOptions & options)
{
    if (options.temperature)
        generate_opts.temperature = options.temperature;
    if (options.max_tokens)
        generate_opts.max_tokens = options.max_tokens;
    if (options.top_p)
        generate_opts.top_p = options.top_p;
    if (options.seed)
        generate_opts.seed = options.seed;
    if (options.frequency_penalty)
        generate_opts.frequency_penalty = options.frequency_penalty;
    if (options.presence_penalty)
        generate_opts.presence_penalty = options.presence_penalty;
}

/// RequestRateLimiter implementation

RequestRateLimiter::RequestRateLimiter(size_t requests_per_minute)
{
    if (requests_per_minute > 0)
        min_interval_ms = static_cast<uint64_t>(60000.0 / static_cast<double>(requests_per_minute));
}

void RequestRateLimiter::wait()
{
    if (min_interval_ms == 0)
        return;

    std::lock_guard<std::mutex> lock(mutex);
    auto now = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_request_time).count();
    if (elapsed_ms < static_cast<long long>(min_interval_ms))
        sleepForMilliseconds(min_interval_ms - static_cast<uint64_t>(elapsed_ms));
    last_request_time = std::chrono::steady_clock::now();
}

}
