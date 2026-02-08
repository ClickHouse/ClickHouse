#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <Core/ColumnNumbers.h>

#if USE_EMBEDDED_COMPILER
#include <DataTypes/Native.h>
#endif


namespace DB
{
namespace
{

/// If value is not Nullable or NULL, wraps it to Nullable.
class FunctionToNullable : public IFunction
{
public:
    static constexpr auto name = "toNullable";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionToNullable>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    /// Disable the default LowCardinality handling to preserve nested LowCardinality in compound types
    /// (e.g., Tuple(LowCardinality(UInt8), UInt8)). The default implementation would recursively strip
    /// LowCardinality from all nested types, which is incorrect for toNullable - it should only wrap
    /// the type in Nullable without modifying inner types.
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        /// For top-level LowCardinality, convert LowCardinality(T) to LowCardinality(Nullable(T))
        if (arguments[0]->lowCardinality())
        {
            const auto & lc_type = assert_cast<const DataTypeLowCardinality &>(*arguments[0]);
            return std::make_shared<DataTypeLowCardinality>(makeNullable(lc_type.getDictionaryType()));
        }
        return makeNullable(arguments[0]);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override
    {
        /// For LowCardinality columns, convert to full, wrap in Nullable, then re-encode as LowCardinality.
        /// We cannot use ColumnLowCardinality::cloneNullable() here because it turns index-0 (the default
        /// value, e.g. integer 0) into NULL, which would silently corrupt data.
        if (arguments[0].type->lowCardinality())
        {
            auto full_column = arguments[0].column->convertToFullColumnIfLowCardinality();
            auto nullable_full = makeNullable(full_column);
            auto res_column = result_type->createColumn();
            auto & lc_res = assert_cast<ColumnLowCardinality &>(*res_column);
            lc_res.insertRangeFromFullColumn(*nullable_full, 0, nullable_full->size());
            return res_column;
        }
        return makeNullable(arguments[0].column);
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes & arguments, const DataTypePtr &) const override { return canBeNativeType(arguments[0]); }

    llvm::Value *
    compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & result_type) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        return nativeCast(b, arguments[0], result_type);
    }
#endif


};

}

REGISTER_FUNCTION(ToNullable)
{
    FunctionDocumentation::Description description = R"(
Converts the provided argument type to `Nullable`.
    )";
    FunctionDocumentation::Syntax syntax = "toNullable(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "A value of any non-compound type.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the input value but of `Nullable` type.", {"Nullable(Any)"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example",
         R"(
SELECT toTypeName(10), toTypeName(toNullable(10));
        )",
         R"(
┌─toTypeName(10)─┬─toTypeName(toNullable(10))─┐
│ UInt8          │ Nullable(UInt8)            │
└────────────────┴────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in{1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Null;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToNullable>(documentation);
}

}
