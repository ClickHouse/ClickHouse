#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Native.h>

#include "config.h"

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#endif
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>


namespace DB
{
namespace
{

/// Implements the function nullIf which takes 2 arguments and returns
/// NULL if both arguments have the same value. Otherwise it returns the
/// value of the first argument.
class FunctionNullIf final : public IFunction
{
public:
    static constexpr auto name = "nullIf";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionNullIf>(context);
    }

    explicit FunctionNullIf(ContextPtr context)
        : equals_resolver(FunctionFactory::instance().get("equals", context))
        , if_resolver(FunctionFactory::instance().get("if", context))
        , comparison_params(context ? ComparisonParams(context) : ComparisonParams())
    {
    }

    /// The compiled `equals` is built from the same context, so its key is the key of `equals`.
    void updateHash(SipHash & hash) const override { comparison_params.updateHash(hash); }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    String getSignatureString() const override { return "(T, Any) -> Nullable(T)"; }

#if USE_EMBEDDED_COMPILER
    /// `if(equals(x, y), NULL, x)`, with the compiled `equals` that `executeImpl` would build.
    FunctionBasePtr buildEquals(const DataTypes & types) const
    {
        return equals_resolver->build({{nullptr, types[0], ""}, {nullptr, types[1], ""}});
    }

    bool isCompilableImpl(const DataTypes & types, const DataTypePtr & result_type) const override
    {
        return canBeNativeType(*result_type) && canBeNativeType(*types[0]) && canBeNativeType(*types[1]) && buildEquals(types)->isCompilable();
    }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & result_type) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        auto equals = buildEquals({arguments[0].type, arguments[1].type});
        auto * equal = equals->compile(builder, arguments);

        /// A NULL comparison is a false condition of `if`.
        llvm::Value * is_equal = nullptr;
        if (equals->getResultType()->isNullable())
            is_equal = b.CreateAnd(b.CreateNot(b.CreateExtractValue(equal, {1})), b.CreateICmpNE(b.CreateExtractValue(equal, {0}), b.getInt8(0)));
        else
            is_equal = b.CreateICmpNE(equal, b.getInt8(0));

        llvm::Value * value = arguments[0].value;
        llvm::Value * is_null = b.getFalse();
        if (arguments[0].type->isNullable())
        {
            value = b.CreateExtractValue(arguments[0].value, {0});
            is_null = b.CreateExtractValue(arguments[0].value, {1});
        }

        /// `if` with a NULL branch keeps the value of the other branch under the NULL (`executeForNullThenElse`).
        auto * result = llvm::Constant::getNullValue(toNativeType(b, result_type));
        return b.CreateInsertValue(b.CreateInsertValue(result, value, {0}), b.CreateOr(is_equal, is_null), {1});
    }
#endif

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// nullIf(col1, col2) == if(col1 = col2, NULL, col1)

        auto equals_func = equals_resolver->build(arguments);
        auto eq_res = equals_func->execute(arguments, equals_func->getResultType(), input_rows_count, /* dry_run = */ false);

        ColumnsWithTypeAndName if_columns
        {
            {eq_res, equals_func->getResultType(), ""},
            {result_type->createColumnConstWithDefaultValue(input_rows_count), result_type, "NULL"},
            arguments[0],
        };

        auto func_if = if_resolver->build(if_columns);
        auto if_res = func_if->execute(if_columns, result_type, input_rows_count, /* dry_run = */ false);

        return makeNullable(if_res);
    }

private:
    FunctionOverloadResolverPtr equals_resolver;
    FunctionOverloadResolverPtr if_resolver;
    const ComparisonParams comparison_params;
};

}

REGISTER_FUNCTION(NullIf)
{
    FunctionDocumentation::Description description = R"(
Returns `NULL` if both arguments are equal.
    )";
    FunctionDocumentation::Syntax syntax = "nullIf(x, y)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The first value.", {"Any"}},
        {"y", "The second value.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `NULL` if both arguments are equal, otherwise returns the first argument.", {"NULL", "Nullable(x)"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example",
         R"(
SELECT nullIf(1, 1), nullIf(1, 2);
        )",
         R"(
┌─nullIf(1, 1)─┬─nullIf(1, 2)─┐
│         ᴺᵁᴸᴸ │            1 │
└──────────────┴──────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in{1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Null;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionNullIf>(documentation, FunctionFactory::Case::Insensitive);
}

}
