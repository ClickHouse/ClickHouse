#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStartsEndsWith.h>


namespace DB
{

using FunctionStartsWithUTF8 = FunctionStartsEndsWith<NameStartsWithUTF8>;

REGISTER_FUNCTION(StartsWithUTF8)
{
    factory.registerFunction<FunctionStartsWithUTF8>(FunctionDocumentation{
        .description = R"(
Returns whether string `str` starts with `prefix`, the difference between `startsWithUTF8` and `startsWith` is that `startsWithUTF8` match `str` and `suffix` by UTF-8 characters.
        )",
        .examples{{"startsWithUTF8", "select startsWithUTF8('富强民主文明和谐', '富强');", ""}},
        .category{"Strings"}});
}

}
