#include <Functions/FunctionFactory.h>
#include <Functions/hasTokenIndex.h>

namespace DB
{

REGISTER_FUNCTION(HasTokenIndex)
{
    factory.registerFunction<FunctionHasTokenIndex>(
        FunctionDocumentation{
            .description="Counterpart of hasToken but performs a token search in the text index. This is an internal function, do not use it from SQL.",
            .arguments = {
                {"index_name", "The name of the text index.", {"String"}},
                {"token", "Token to search for.", {"String"}},
                {"_part_index", "The internal virtual column with this name"},
                {"_part_offset", "The internal virtual column with this name"},
            },
            .returned_value = {"Returns 0 (no match) or 1 (match)."},
            .introduced_in = {25, 8},
            .category = FunctionDocumentation::Category::StringSearch,
        });
}

}
