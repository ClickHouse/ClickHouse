#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStartsEndsWith.h>


namespace DB
{

using FunctionEndsWithUTF8 = FunctionStartsEndsWith<NameEndsWithUTF8>;

REGISTER_FUNCTION(EndsWithUTF8)
{
    factory.registerFunction<FunctionEndsWithUTF8>(FunctionDocumentation{
        .description = R"(
Returns whether string `str` ends with `suffix`, the difference between `endsWithUTF8` and `endsWith` is that `endsWithUTF8` match `str` and `suffix` by UTF-8 characters.
        )",
        .examples{{"endsWithUTF8", "select endsWithUTF8('富强民主文明和谐', '富强');", ""}},
        .categories{"String"}});
}

}
