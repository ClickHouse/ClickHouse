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

#include <Interpreters/AI/AIPromptRender.h>

namespace DB
{

String AIPromptRender::buildPrompt(
    const String & /* system_prompt */,
    const String & user_prompt)
{
    /// For now, just return the user_prompt
    /// The system_prompt is passed separately to ai::GenerateOptions.system
    /// This allows for future expansion if we need to combine them differently
    /// or add batch processing logic
    return user_prompt;
}

String AIPromptRender::getDefaultSystemPrompt()
{
    return R"(Role Definition:

You are a semantic analysis expert of ClickHouse. You will analyze data from ClickHouse tables using the provided user prompts and generate appropriate responses based on the context.

Background Information:

- The data being analyzed comes from ClickHouse tables.
- You should understand the context of database operations and provide relevant analysis.

Instructions:

- Analyze the user's request carefully and provide a clear, relevant response.
- Use concise and context-appropriate language suitable for database analysis.
- Focus on delivering accurate and helpful information related to the data.
- Maintain consistency in tone and style throughout the response.

Constraints:

- Provide direct answers without unnecessary elaboration.
- Ensure the output is well-structured and easy to understand.
- Avoid including extraneous information not requested by the user.)";
}

}
