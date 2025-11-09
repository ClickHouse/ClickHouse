#include <Interpreters/LLM/PromptRender.h>
#include <Columns/ColumnString.h>
#include <nlohmann/json.hpp>

namespace DB
{

void PromptTemplate::render(WriteBufferFromOwnString & text, const String & prompt, const ColumnString::Chars & data, const ColumnString::Offsets & offsets, size_t offset, size_t size)
{
    writeText(R"(
Role Definition:

You are a semantic analysis expert of ClickHouse. You will analyze each line of input data using the provided user prompt and output the analysis conclusions in the specified format.

Background Information:

- The data being processed comes from a string column in the dataset. Each input row is a string.)", text);

    writeText(fmt::format(R"("

User prompt:

- {})", prompt), text);

    writeText(R"(

Input Data:

)", text);

    auto tuples = nlohmann::json::array();
    size_t prev_offset = (offset == 0) ? 0 : offsets[offset - 1];
    size_t limit = offset + size;
    for (size_t i = offset; i < limit; ++i)
    {
        std::string_view element(reinterpret_cast<const char*>(&data[prev_offset]), offsets[i] - prev_offset - 1);
        prev_offset = offsets[i];
        writeText(fmt::format("- {}\n", element), text);
    }

    writeText(R"(

Analysis Instructions:

- The response should be directly relevant to each tuple without additional formatting, purely answering the user prompt as if each tuple were a standalone entity.
- Use clear, context-relevant language to generate a meaningful and concise answer for each tuple.

)", text);

    writeText(R"(
Expected Output Format:

- Analyze the content of each input string based on the user prompt and output it in text form, including only a newline at the end of the text.
- Ensure no tuple is missed.
- The output should not contain any newline characters except for the line breaks.

Constraints:

- Analyze each row of data in the Tuples without any omissions.
- The returned format should be easy for the program to parse.
    )", text);
}
}
