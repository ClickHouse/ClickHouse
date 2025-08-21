#include <Functions/FunctionFactory.h>
#include <Functions/hasTokenIndex.h>

namespace DB
{

REGISTER_FUNCTION(HasTokenIndex)
{
    factory.registerFunction<FunctionHasTokenIndex>(
        FunctionDocumentation{
            .description="Performs token search using only index. Optimized equivalent of hasToken. This function is intended to be used mainly internally.",
            .arguments = {
                {"index_name", "The name of the text index to use. This should be the name of a text index associated with the column argument of hasToken.", {"String"}},
                {"token", "Token to search for in the column.", {"String"}},
                {"_part_index", "The internal virtual column with this name"},
                {"_part_offset", "The internal virtual column with this name"},
            },
            .returned_value = {"Returns 0 or 1 depending for filtering purposes is the row has 'token'."},
            .introduced_in = {25, 8},
            .category = FunctionDocumentation::Category::StringSearch,
        },
        DB::FunctionFactory::Case::Insensitive);
}

}
