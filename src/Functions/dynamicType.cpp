#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadBufferFromMemory.h>
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

/// Return String with type name for each row in Dynamic column.
class FunctionDynamicType : public IFunction
{
public:
    static constexpr auto name = "dynamicType";
    static constexpr auto name_for_null = "None";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDynamicType>(); }
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

        if (!isDynamic(arguments[0].type.get()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be Dynamic, got {} instead",
                getName(), arguments[0].type->getName());

        return std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnDynamic * dynamic_column = checkAndGetColumn<ColumnDynamic>(arguments[0].column.get());
        if (!dynamic_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument for function {} must be Dynamic, got {} instead",
                            getName(), arguments[0].type->getName());

        const auto & variant_info = dynamic_column->getVariantInfo();
        const auto & variant_column = dynamic_column->getVariantColumn();
        auto res = result_type->createColumn();
        String element_type;
        auto shared_variant_discr = dynamic_column->getSharedVariantDiscriminator();
        const auto & shared_variant = dynamic_column->getSharedVariant();
        for (size_t i = 0; i != input_rows_count; ++i)
        {
            auto global_discr = variant_column.globalDiscriminatorAt(i);
            if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
                element_type = name_for_null;
            else if (global_discr == shared_variant_discr)
                element_type = getTypeNameFromSharedVariantValue(shared_variant.getDataAt(variant_column.offsetAt(i)));
            else
                element_type = variant_info.variant_names[global_discr];

            res->insertData(element_type.data(), element_type.size());
        }

        return res;
    }

    String getTypeNameFromSharedVariantValue(StringRef value) const
    {
        ReadBufferFromMemory buf(value.data, value.size);
        return decodeDataType(buf)->getName();
    }
};

class FunctionIsDynamicElementInSharedData : public IFunction
{
public:
    static constexpr auto name = "isDynamicElementInSharedData";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIsDynamicElementInSharedData>(); }
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

        if (!isDynamic(arguments[0].type.get()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be Dynamic, got {} instead",
                getName(), arguments[0].type->getName());

        return DataTypeFactory::instance().get("Bool");
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnDynamic * dynamic_column = checkAndGetColumn<ColumnDynamic>(arguments[0].column.get());
        if (!dynamic_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument for function {} must be Dynamic, got {} instead",
                            getName(), arguments[0].type->getName());

        const auto & variant_column = dynamic_column->getVariantColumn();
        const auto & local_discriminators = variant_column.getLocalDiscriminators();
        auto res = result_type->createColumn();
        auto & res_data = assert_cast<ColumnUInt8 &>(*res).getData();
        res_data.reserve(dynamic_column->size());
        auto shared_variant_local_discr = variant_column.localDiscriminatorByGlobal(dynamic_column->getSharedVariantDiscriminator());
        for (size_t i = 0; i != input_rows_count; ++i)
            res_data.push_back(local_discriminators[i] == shared_variant_local_discr);

        return res;
    }
};

}

REGISTER_FUNCTION(DynamicType)
{
    factory.registerFunction<FunctionDynamicType>(FunctionDocumentation{
        .description = R"(
Returns the variant type name for each row of `Dynamic` column. If row contains NULL, it returns 'None' for it.
)",
        .syntax = {"dynamicType(dynamic)"},
        .arguments = {{"dynamic", "Dynamic column"}},
        .examples = {{{
            "Example",
            R"(
CREATE TABLE test (d Dynamic) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT d, dynamicType(d) FROM test;
)",
            R"(
┌─d─────────────┬─dynamicType(d)─┐
│ ᴺᵁᴸᴸ          │ None           │
│ 42            │ Int64          │
│ Hello, World! │ String         │
│ [1,2,3]       │ Array(Int64)   │
└───────────────┴────────────────┘
)"}}},
        .categories{"Variant"},
    });

    factory.registerFunction<FunctionIsDynamicElementInSharedData>(FunctionDocumentation{
        .description = R"(
Returns true for rows in Dynamic column that are not separated into subcolumns and stored inside shared variant in binary form.
)",
        .syntax = {"isDynamicElementInSharedData(dynamic)"},
        .arguments = {{"dynamic", "Dynamic column"}},
        .examples = {{{
            "Example",
            R"(
CREATE TABLE test (d Dynamic(max_types=2)) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT d, isDynamicElementInSharedData(d) FROM test;
)",
            R"(
┌─d─────────────┬─isDynamicElementInSharedData(d)─┐
│ ᴺᵁᴸᴸ          │ false              │
│ 42            │ false              │
│ Hello, World! │ true               │
│ [1,2,3]       │ true               │
└───────────────┴────────────────────┘
)"}}},
        .categories{"Variant"},
    });
}

}
