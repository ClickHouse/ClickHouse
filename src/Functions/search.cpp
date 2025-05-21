#include "Functions/search.h"

namespace DB
{

REGISTER_FUNCTION(SearchAny)
{
    factory.registerFunction<FunctionSearchImpl<details::SearchAnyProps>>(FunctionDocumentation{
        .description = "Searches the needle tokens in the generated tokens from the text by a given tokenizer. Returns true if any needle "
                       "tokens exists in the text, otherwise false.",
        .category = FunctionDocumentation::Category::StringSearch});
}

REGISTER_FUNCTION(SearchAll)
{
    factory.registerFunction<FunctionSearchImpl<details::SearchAllProps>>(FunctionDocumentation{
        .description = "Searches the needle tokens in the generated tokens from the text by a given tokenizer. Returns true if all needle "
                       "tokens exists in the text, otherwise false.",
        .category = FunctionDocumentation::Category::StringSearch});
}
}
