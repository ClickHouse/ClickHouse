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

#include <base/types.h>

namespace DB
{

/// Renders prompts for AI text generation
/// Handles prompt construction and formatting
class AIPromptRender
{
public:
    /// Build a simple prompt from system and user prompts
    /// Currently just returns the user_prompt as the system_prompt
    /// is passed separately to ai::GenerateOptions
    /// @param system_prompt - System-level instructions for the LLM (unused for now)
    /// @param user_prompt - User's actual prompt/question
    /// @return Prompt ready for LLM
    static String buildPrompt(
        const String & system_prompt,
        const String & user_prompt);

    /// Get default system prompt when none is provided
    /// @return Default system prompt with role definition and instructions
    static String getDefaultSystemPrompt();

    // Future: Add batch prompt rendering similar to PR #85442
    // static String buildBatchPrompt(
    //     const String & system_prompt,
    //     const std::vector<String> & user_prompts);
};

}
