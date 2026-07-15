#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/Native.h>
#    include <llvm/IR/IRBuilder.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Implements the function assumeNotNull which takes 1 argument and works as follows:
/// - if the argument is a nullable column, return its embedded column;
/// - otherwise return the original argument.
/// NOTE: assumeNotNull may not be called with the NULL value.
class FunctionAssumeNotNull : public IFunction
{
public:
    static constexpr auto name = "assumeNotNull";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionAssumeNotNull>();
    }

    std::string getName() const override
    {
        return name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    bool hasInformationAboutMonotonicity() const override { return true; }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & /*left*/, const Field & right) const override
    {
        /// assumeNotNull() is identity for non-Nullable values, so it preserves ordering and thus monotonic.
        /// For Nullable, treat it as monotonic only when the analyzed range is guaranteed to not contain
        /// NULLs. NULLs always represented as POSITIVE_INFINITY and they will always be at the end of ordering.
        /// So, we do not need to check left.isNull().
        bool can_contain_null = canContainNull(type);
        if (can_contain_null && right.isNull())
            return {};

        return { .is_monotonic = true, .is_positive = true, .is_always_monotonic = !can_contain_null };
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return removeNullable(arguments[0].type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const ColumnPtr & col = arguments[0].column;

        if (arguments[0].type->onlyNull() && !col->empty())
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot create non-empty column with type Nothing");

        if (const auto * nullable_col = checkAndGetColumn<ColumnNullable>(&*col))
            return nullable_col->getNestedColumnPtr();
        return col;
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes & arguments, const DataTypePtr &) const override { return canBeNativeType(arguments[0]); }

    llvm::Value *
    compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & /*result_type*/) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        if (arguments[0].type->isNullable())
            return b.CreateExtractValue(arguments[0].value, {0});
        else
            return arguments[0].value;
    }
#endif


};

}

REGISTER_FUNCTION(AssumeNotNull)
{
    FunctionDocumentation::Description description = R"(
Returns the corresponding non-`Nullable` value for a value of type [`Nullable`](../data-types/nullable.md).
If the original value is `NULL`, an arbitrary result can be returned.

See also: functions [`ifNull`](#ifNull) and [`coalesce`](#coalesce).
    )";
    FunctionDocumentation::Syntax syntax = "assumeNotNull(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The original value of any nullable type.", {"Nullable(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the non-nullable value, if the original value was not `NULL`, otherwise an arbitrary value, if the input value is `NULL`.", {"Any"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example",
         R"(
CREATE TABLE t_null (x Int8, y Nullable(Int8))
ENGINE=MergeTree()
ORDER BY x;

INSERT INTO t_null VALUES (1, NULL), (2, 3);

SELECT assumeNotNull(y) FROM table;
SELECT toTypeName(assumeNotNull(y)) FROM t_null;
        )",
         R"(
┌─assumeNotNull(y)─┐
│                0 │
│                3 │
└──────────────────┘
┌─toTypeName(assumeNotNull(y))─┐
│ Int8                         │
│ Int8                         │
└──────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in{1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Null;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAssumeNotNull>(documentation);
}

}
