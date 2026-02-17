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
    FunctionDocumentation::Description dynamicType_description = R"(
Returns the variant type name for each row of a `Dynamic` column.

For rows containing NULL, the function returns 'None'. For all other rows, it returns the actual data type
stored in that row of the Dynamic column (e.g., 'Int64', 'String', 'Array(Int64)').
)";
    FunctionDocumentation::Syntax dynamicType_syntax = "dynamicType(dynamic)";
    FunctionDocumentation::Arguments dynamicType_arguments =
    {
        {"dynamic", "Dynamic column to inspect.", {"Dynamic"}}
    };
    FunctionDocumentation::ReturnedValue dynamicType_returned_value =
    {
        "Returns the type name of the value stored in each row, or 'None' for NULL values.",
        {"String"}
    };
    FunctionDocumentation::Examples dynamicType_examples =
    {
    {
        "Inspecting types in Dynamic column",
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
        )"
    }
    };
    FunctionDocumentation::IntroducedIn dynamicType_introduced_in = {24, 1};
    FunctionDocumentation::Category dynamicType_category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation dynamicType_documentation = {dynamicType_description, dynamicType_syntax, dynamicType_arguments, dynamicType_returned_value, dynamicType_examples, dynamicType_introduced_in, dynamicType_category};

    factory.registerFunction<FunctionDynamicType>(dynamicType_documentation);

    FunctionDocumentation::Description isDynamicElementInSharedData_description = R"(
Returns true for rows in a Dynamic column that are stored in shared variant format rather than as separate subcolumns.

When a Dynamic column has a `max_types` limit, values that exceed this limit are stored in a shared binary format
instead of being separated into individual typed subcolumns. This function identifies which rows are stored in this shared format.
    )";
    FunctionDocumentation::Syntax isDynamicElementInSharedData_syntax = "isDynamicElementInSharedData(dynamic)";
    FunctionDocumentation::Arguments isDynamicElementInSharedData_arguments =
    {
        {"dynamic", "Dynamic column to inspect.", {"Dynamic"}}
    };
    FunctionDocumentation::ReturnedValue isDynamicElementInSharedData_returned_value =
    {
        "Returns true if the value is stored in shared variant format, false if stored as a separate subcolumn or is NULL.",
        {"Bool"}
    };
    FunctionDocumentation::Examples isDynamicElementInSharedData_examples =
    {
    {
        "Checking storage format in Dynamic column with max_types limit",
        R"(
CREATE TABLE test (d Dynamic(max_types=2)) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT d, isDynamicElementInSharedData(d) FROM test;
        )",
        R"(
┌─d─────────────┬─isDynamicElementInSharedData(d)─┐
│ ᴺᵁᴸᴸ          │ false                           │
│ 42            │ false                           │
│ Hello, World! │ true                            │
│ [1,2,3]       │ true                            │
└───────────────┴─────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn isDynamicElementInSharedData_introduced_in = {24, 1};
    FunctionDocumentation::Category isDynamicElementInSharedData_category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation isDynamicElementInSharedData_documentation = {isDynamicElementInSharedData_description, isDynamicElementInSharedData_syntax, isDynamicElementInSharedData_arguments, isDynamicElementInSharedData_returned_value, isDynamicElementInSharedData_examples, isDynamicElementInSharedData_introduced_in, isDynamicElementInSharedData_category};

    factory.registerFunction<FunctionIsDynamicElementInSharedData>(isDynamicElementInSharedData_documentation);
}

}
