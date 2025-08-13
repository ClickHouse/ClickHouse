#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameLocate
{
    static constexpr auto name = "locate";
};

using FunctionLocate = FunctionsStringSearch<PositionImpl<NameLocate, PositionCaseSensitiveASCII>, ExecutionErrorPolicy::Throw, HaystackNeedleOrderIsConfigurable::Yes>;

}

REGISTER_FUNCTION(Locate)
{
    FunctionDocumentation::Description doc_description = "Like function `position` but with arguments `haystack` and `locate` switched. The behavior of this function depends on the ClickHouse version: In versions < v24.3, `locate` was an alias of function `position` and accepted arguments `(haystack, needle[, start_pos])`. In versions >= 24.3,, `locate` is an individual function (for better compatibility with MySQL) and accepts arguments `(needle, haystack[, start_pos])`. The previous behavior can be restored using setting `function_locate_has_mysql_compatible_argument_order = false`.";
    FunctionDocumentation::Syntax doc_syntax = "location(needle, haystack[, start_pos])";
    FunctionDocumentation::Arguments doc_arguments = {{"needle", "Substring to be searched (String)"},
                                                      {"haystack", "String in which the search is performed (String)."},
                                                      {"start_pos", "Position (1-based) in `haystack` at which the search starts (UInt*)."}};
    FunctionDocumentation::ReturnedValue doc_returned_value = "Starting position in bytes and counting from 1, if the substring was found. 0, if the substring was not found.";
    FunctionDocumentation::Examples doc_examples = {{"Example", "SELECT locate('abcabc', 'ca');", "3"}};
    FunctionDocumentation::Categories doc_categories = {"String search"};


    factory.registerFunction<FunctionLocate>({doc_description, doc_syntax, doc_arguments, doc_returned_value, doc_examples, doc_categories}, FunctionFactory::Case::Insensitive);
}
}
