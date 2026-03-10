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

#include <optional>
#include <string>
#include <base/types.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

/// Options for AI text generation requests
struct AIRequestOptions
{
    /// Provider and model
    std::optional<String> provider;
    std::optional<String> model;

    /// API configuration
    std::optional<String> api_key;
    std::optional<String> base_url;

    /// Generation parameters
    std::optional<double> temperature;
    std::optional<int> max_tokens;
    std::optional<double> top_p;
    std::optional<int> seed;
    std::optional<double> frequency_penalty;
    std::optional<double> presence_penalty;

    /// Control parameters
    size_t requests_per_minute = 0;
    size_t max_retries = 3;
    size_t retry_delay_ms = 1000;

    /// Parse options from JSON string
    /// @throws Exception if JSON is invalid
    static AIRequestOptions fromJSON(const String & json_str);

    /// Merge with server configuration
    /// Config values are used as fallback when option is not set
    void mergeWithConfig(const Poco::Util::AbstractConfiguration & config);

    /// Validate options
    /// @throws Exception if options are invalid (e.g., provider or model missing)
    void validate() const;
};

}
