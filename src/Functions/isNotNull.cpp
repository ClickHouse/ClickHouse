#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVariant.h>
#include <Core/ColumnNumbers.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

namespace
{

/// Implements the function isNotNull which returns true if a value
/// is not null, false otherwise.
class FunctionIsNotNull : public IFunction
{
public:
    static constexpr auto name = "isNotNull";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionIsNotNull>(context->getSettingsRef()[Setting::allow_experimental_analyzer]);
    }

    explicit FunctionIsNotNull(bool use_analyzer_) : use_analyzer(use_analyzer_) {}

    std::string getName() const override
    {
        return name;
    }

    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        /// (column IS NULL) triggers a bug in old analyzer when it is replaced to constant.
        if (!use_analyzer)
            return nullptr;

        const ColumnWithTypeAndName & elem = arguments[0];
        if (elem.type->onlyNull())
            return result_type->createColumnConst(1, UInt8(0));

        if (canContainNull(*elem.type))
            return nullptr;

        return result_type->createColumnConst(1, UInt8(1));
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & elem = arguments[0];

        if (isVariant(elem.type) || isDynamic(elem.type))
        {
            const auto & column_variant = isVariant(elem.type) ? checkAndGetColumn<ColumnVariant>(*elem.column) : checkAndGetColumn<ColumnDynamic>(*elem.column).getVariantColumn();
            const auto & discriminators = column_variant.getLocalDiscriminators();
            auto res = DataTypeUInt8().createColumn();
            auto & data = typeid_cast<ColumnUInt8 &>(*res).getData();
            data.resize(discriminators.size());
            for (size_t i = 0; i < discriminators.size(); ++i)
                data[i] = discriminators[i] != ColumnVariant::NULL_DISCRIMINATOR;
            return res;
        }

        if (elem.type->isLowCardinalityNullable())
        {
            const auto & low_cardinality_column = checkAndGetColumn<ColumnLowCardinality>(*elem.column);
            const size_t null_index = low_cardinality_column.getDictionary().getNullValueIndex();
            auto res = DataTypeUInt8().createColumn();
            auto & data = typeid_cast<ColumnUInt8 &>(*res).getData();
            data.resize(low_cardinality_column.size());
            for (size_t i = 0; i != low_cardinality_column.size(); ++i)
                data[i] = (low_cardinality_column.getIndexAt(i) != null_index);
            return res;
        }

        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&*elem.column))
        {
            /// Return the negated null map.
            auto res_column = ColumnUInt8::create(input_rows_count);
            const auto & src_data = nullable->getNullMapData();
            auto & res_data = assert_cast<ColumnUInt8 &>(*res_column).getData();
            vector(src_data, res_data);
            return res_column;
        }

        /// Since no element is nullable, return a constant one.
        return DataTypeUInt8().createColumnConst(elem.column->size(), 1u);
    }

private:
    MULTITARGET_FUNCTION_AVX2_SSE42(
    MULTITARGET_FUNCTION_HEADER(static void NO_INLINE), vectorImpl, MULTITARGET_FUNCTION_BODY((const PaddedPODArray<UInt8> & null_map, PaddedPODArray<UInt8> & res) /// NOLINT
    {
        size_t size = null_map.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = !null_map[i];
    }))

    static void NO_INLINE vector(const PaddedPODArray<UInt8> & null_map, PaddedPODArray<UInt8> & res)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX2))
        {
            vectorImplAVX2(null_map, res);
            return;
        }

        if (isArchSupported(TargetArch::SSE42))
        {
            vectorImplSSE42(null_map, res);
            return;
        }
#endif
        vectorImpl(null_map, res);
    }

    bool use_analyzer;
};
}

REGISTER_FUNCTION(IsNotNull)
{
    FunctionDocumentation::Description description = R"(
Checks if the argument is not `NULL`.

Also see: operator [`IS NOT NULL`](/sql-reference/operators#is_not_null).
    )";
    FunctionDocumentation::Syntax syntax = "isNotNull(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "A value of non-compound data type.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `x` is not `NULL`, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
CREATE TABLE t_null
(
  x Int32,
  y Nullable(Int32)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_null VALUES (1, NULL), (2, 3);

SELECT x FROM t_null WHERE isNotNull(y);
        )",
        R"(
┌─x─┐
│ 2 │
└───┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Null;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIsNotNull>(documentation);
}

}
