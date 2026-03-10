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

#pragma once

#include <Interpreters/AI/AIRequestOptions.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>
#include <chrono>
#include <mutex>

namespace ai
{
class Client;
struct GenerateOptions;
}

namespace DB
{

/// Core class for AI text generation
/// Handles SDK calls, rate limiting, and retry logic
class AITextGenerator
{
public:
    explicit AITextGenerator(ContextPtr context_);

    /// Generate text for a column of prompts
    /// @param system_prompt - System prompt for the LLM
    /// @param user_prompts - Column containing user prompts
    /// @param options - Request options (must be validated)
    /// @param input_rows_count - Number of rows to process
    /// @return Column containing generated text
    ColumnPtr generateText(
        const String & system_prompt,
        const ColumnString & user_prompts,
        const AIRequestOptions & options,
        size_t input_rows_count) const;

private:
    ContextPtr context;

    /// Generate text for a single prompt with retry logic
    /// @throws Exception on failure after all retries
    String generateSingle(
        ai::Client & client,
        const String & model,
        const String & system_prompt,
        const String & user_prompt,
        const AIRequestOptions & options) const;

    /// Check if error is retryable (429, 5xx)
    static bool isRetryableError(const String & error_msg);

    /// Apply generation options to ai::GenerateOptions
    static void applyOptions(
        ai::GenerateOptions & generate_opts,
        const AIRequestOptions & options);
};

/// Simple rate limiter for API requests
class RequestRateLimiter
{
public:
    explicit RequestRateLimiter(size_t requests_per_minute);

    /// Wait if necessary to respect rate limit
    void wait();

private:
    uint64_t min_interval_ms = 0;
    std::chrono::steady_clock::time_point last_request_time;
    std::mutex mutex;
};

}
