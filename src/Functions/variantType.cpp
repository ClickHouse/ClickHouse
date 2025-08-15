#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeEnum.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Return enum with type name for each row in Variant column.
class FunctionVariantType : public IFunction
{
public:
    static constexpr auto name = "variantType";
    static constexpr auto enum_name_for_null = "None";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionVariantType>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 1",
                getName(), arguments.empty());

        const DataTypeVariant * variant_type = checkAndGetDataType<DataTypeVariant>(arguments[0].type.get());

        if (!variant_type)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be Variant, got {} instead",
                getName(), arguments[0].type->getName());

        const auto & variants = variant_type->getVariants();
        std::vector<std::pair<String, Int8>> enum_values;
        enum_values.reserve(variants.size() + 1);
        for (ColumnVariant::Discriminator i = 0; i != variants.size(); ++i)
            enum_values.emplace_back(variants[i]->getName(), i);
        enum_values.emplace_back(enum_name_for_null, ColumnVariant::NULL_DISCRIMINATOR);
        return std::make_shared<DataTypeEnum<Int8>>(enum_values);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnVariant * variant_column = checkAndGetColumn<ColumnVariant>(arguments[0].column.get());
        if (!variant_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be Variant, got {} instead",
                getName(), arguments[0].type->getName());

        auto res = result_type->createColumn();
        auto & res_data = typeid_cast<ColumnInt8 *>(res.get())->getData();
        res_data.reserve(input_rows_count);
        for (size_t i = 0; i != input_rows_count; ++i)
            res_data.push_back(variant_column->globalDiscriminatorAt(i));

        return res;
    }
};

}

REGISTER_FUNCTION(VariantType)
{
    factory.registerFunction<FunctionVariantType>(FunctionDocumentation{
        .description = R"(
Returns the variant type name for each row of `Variant` column. If row contains NULL, it returns 'None' for it.
)",
        .syntax = {"variantType(variant)"},
        .arguments = {{"variant", "Variant column"}},
        .examples = {{{
            "Example",
            R"(
CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT variantType(v) FROM test;)",
            R"(
┌─variantType(v)─┐
│ None           │
│ UInt64         │
│ String         │
│ Array(UInt64)  │
└────────────────┘
)"}}},
        .categories{"Variant"},
    });
}

}
