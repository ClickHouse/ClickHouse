#include <Functions/FunctionFactory.h>
#include <Functions/hasTokenIndex.h>

#include <Common/Volnitsky.h>

namespace DB
{

REGISTER_FUNCTION(HasTokenIndex)
{
    factory.registerFunction<FunctionHasTokenIndex>(
        FunctionDocumentation{.description="Performs token search using only index.", .category = FunctionDocumentation::Category::StringSearch},
        DB::FunctionFactory::Case::Insensitive);
}

}
