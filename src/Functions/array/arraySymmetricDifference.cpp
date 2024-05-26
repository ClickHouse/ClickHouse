#include <Functions/FunctionFactory.h>
#include <Functions/array/arrayLogicalFunctionBase.h>


namespace DB
{

class FunctionArraySymmetricDifference : public FunctionArrayLogicalBase<false>
{
public:
    static constexpr auto name = "arraySymmetricDifference";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionArraySymmetricDifference>(context); }

    explicit FunctionArraySymmetricDifference(ContextPtr context_) : FunctionArrayLogicalBase<false>(context_) { }

protected:
    ContextPtr context;
};


REGISTER_FUNCTION(ArraySymmetricDifference)
{
    FunctionDocumentation::Description description
        = "Takes multiple arrays, returns an array with elements which are not present in all source arrays.";
    FunctionDocumentation::Examples examples
        = {{"empty_symmetric_difference", R"(SELECT arraySymmetricDifference([1, 2], [1, 2], [1, 2]);)", R"([])"},
           {"non_empty_symmetric_difference", R"(SELECT arraySymmetricDifference([1, 2], [1, 2], [1, 3]);)", R"([3])"}};
    FunctionDocumentation::Categories categories = {"Array"};
    FunctionDocumentation docs{.description = description, .examples = examples, .categories = categories};

    factory.registerFunction<FunctionArraySymmetricDifference>(docs);
}

}
