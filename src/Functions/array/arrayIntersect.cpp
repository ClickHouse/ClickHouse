#include <Functions/FunctionFactory.h>
#include <Functions/array/arrayLogicalFunctionBase.h>


namespace DB
{

class FunctionArrayIntersect : public FunctionArrayLogicalBase<true>
{
public:
    static constexpr auto name = "arrayIntersect";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionArrayIntersect>(context); }

    FunctionArrayIntersect(ContextPtr context_) : FunctionArrayLogicalBase<true>(context_) { }

protected:
    ContextPtr context;
};


REGISTER_FUNCTION(ArrayIntersect)
{
    FunctionDocumentation::Description description
        = "Takes multiple arrays, returns an array with elements that are present in all source arrays.";
    FunctionDocumentation::Examples examples
        = {{"empty_intersection", R"(SELECT arrayIntersect([1, 2], [1, 3], [2, 3]);)", R"([])"},
           {"non_empty_intersection", R"(SELECT arrayIntersect([1, 2], [1, 3], [1, 4]);)", R"([1])"}};
    FunctionDocumentation::Categories categories = {"Array"};
    FunctionDocumentation docs{.description = description, .examples = examples, .categories = categories};

    factory.registerFunction<FunctionArrayIntersect>(docs);
}

}
