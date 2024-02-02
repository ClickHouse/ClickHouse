#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <Common/assert_cast.h>
#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/** Extract element of Variant by variant type name.
  * Also the function looks through Arrays: you can get Array of Variant elements from Array of Variants.
  */
class FunctionVariantElement : public IFunction
{
public:
    static constexpr auto name = "variantElement";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionVariantElement>(); }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 2 || number_of_arguments > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Number of arguments for function {} doesn't match: passed {}, should be 2 or 3",
                            getName(), number_of_arguments);

        size_t count_arrays = 0;
        const IDataType * input_type = arguments[0].type.get();
        while (const DataTypeArray * array = checkAndGetDataType<DataTypeArray>(input_type))
        {
            input_type = array->getNestedType().get();
            ++count_arrays;
        }

        const DataTypeVariant * variant_type = checkAndGetDataType<DataTypeVariant>(input_type);
        if (!variant_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument for function {} must be Variant or Array of Variant. Actual {}",
                    getName(),
                    arguments[0].type->getName());

        std::optional<size_t> variant_global_discr = getVariantGlobalDiscriminator(arguments[1].column, *variant_type, number_of_arguments);
        if (variant_global_discr.has_value())
        {
            DataTypePtr return_type = makeNullableOrLowCardinalityNullableSafe(variant_type->getVariant(variant_global_discr.value()));

            for (; count_arrays; --count_arrays)
                return_type = std::make_shared<DataTypeArray>(return_type);

            return return_type;
        }
        else
            return arguments[2].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & input_arg = arguments[0];
        const IDataType * input_type = input_arg.type.get();
        const IColumn * input_col = input_arg.column.get();

        bool input_arg_is_const = false;
        if (typeid_cast<const ColumnConst *>(input_col))
        {
            input_col = assert_cast<const ColumnConst *>(input_col)->getDataColumnPtr().get();
            input_arg_is_const = true;
        }

        Columns array_offsets;
        while (const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(input_type))
        {
            const ColumnArray * array_col = assert_cast<const ColumnArray *>(input_col);

            input_type = array_type->getNestedType().get();
            input_col = &array_col->getData();
            array_offsets.push_back(array_col->getOffsetsPtr());
        }

        const DataTypeVariant * input_type_as_variant = checkAndGetDataType<DataTypeVariant>(input_type);
        const ColumnVariant * input_col_as_variant = checkAndGetColumn<ColumnVariant>(input_col);
        if (!input_type_as_variant || !input_col_as_variant)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument for function {} must be Variant or array of Variants. Actual {}", getName(), input_arg.type->getName());

        std::optional<size_t> variant_global_discr = getVariantGlobalDiscriminator(arguments[1].column, *input_type_as_variant, arguments.size());

        if (!variant_global_discr.has_value())
            return arguments[2].column;

        const auto & variant_type = input_type_as_variant->getVariant(*variant_global_discr);
        const auto & variant_column = input_col_as_variant->getVariantPtrByGlobalDiscriminator(*variant_global_discr);

        /// If Variant has only NULLs or our variant doesn't have any real values,
        /// just create column with default values and create null mask with 1.
        if (input_col_as_variant->hasOnlyNulls() || variant_column->empty())
        {
            auto res = variant_type->createColumn();

            if (variant_type->lowCardinality())
                assert_cast<ColumnLowCardinality &>(*res).nestedToNullable();

            res->insertManyDefaults(input_col_as_variant->size());
            if (!variant_type->canBeInsideNullable())
                return wrapInArraysAndConstIfNeeded(std::move(res), array_offsets, input_arg_is_const, input_rows_count);

            auto null_map = ColumnUInt8::create();
            auto & null_map_data = null_map->getData();
            null_map_data.resize_fill(input_col_as_variant->size(), 1);
            return wrapInArraysAndConstIfNeeded(ColumnNullable::create(std::move(res), std::move(null_map)), array_offsets, input_arg_is_const, input_rows_count);
        }

        /// If we extract single non-empty column and have no NULLs, then just return this variant.
        if (auto non_empty_local_discr = input_col_as_variant->getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls())
        {
            /// If we were trying to extract some other variant,
            /// it would be empty and we would already processed this case above.
            chassert(input_col_as_variant->globalDiscriminatorByLocal(*non_empty_local_discr) == variant_global_discr);
            return wrapInArraysAndConstIfNeeded(makeNullableOrLowCardinalityNullableSafe(variant_column), array_offsets, input_arg_is_const, input_rows_count);
        }

        /// In general case we should calculate null-mask for variant
        /// according to the discriminators column and expand
        /// variant column by this mask to get a full column (with default values on NULLs)
        const auto & local_discriminators = input_col_as_variant->getLocalDiscriminators();
        auto null_map = ColumnUInt8::create();
        auto & null_map_data = null_map->getData();
        null_map_data.reserve(local_discriminators.size());
        auto variant_local_discr = input_col_as_variant->localDiscriminatorByGlobal(*variant_global_discr);
        for (auto local_discr : local_discriminators)
            null_map_data.push_back(local_discr != variant_local_discr);

        auto expanded_variant_column = IColumn::mutate(variant_column);
        if (variant_type->lowCardinality())
            expanded_variant_column = assert_cast<ColumnLowCardinality &>(*expanded_variant_column).cloneNullable();
        expanded_variant_column->expand(null_map_data, /*inverted = */ true);
        if (variant_type->canBeInsideNullable())
            return wrapInArraysAndConstIfNeeded(ColumnNullable::create(std::move(expanded_variant_column), std::move(null_map)), array_offsets, input_arg_is_const, input_rows_count);
        return wrapInArraysAndConstIfNeeded(std::move(expanded_variant_column), array_offsets, input_arg_is_const, input_rows_count);
    }
private:
    std::optional<size_t> getVariantGlobalDiscriminator(const ColumnPtr & index_column, const DataTypeVariant & variant_type, size_t argument_size) const
    {
        const auto * name_col = checkAndGetColumnConst<ColumnString>(index_column.get());
        if (!name_col)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Second argument to {} with Variant argument must be a constant String",
                            getName());

        String variant_element_name = name_col->getValue<String>();
        auto variant_element_type = DataTypeFactory::instance().tryGet(variant_element_name);
        if (variant_element_type)
        {
            const auto & variants = variant_type.getVariants();
            for (size_t i = 0; i != variants.size(); ++i)
            {
                if (variants[i]->getName() == variant_element_type->getName())
                    return i;
            }
        }

        if (argument_size == 2)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} doesn't contain variant with type {}", variant_type.getName(), variant_element_name);
        return std::nullopt;
    }

    ColumnPtr wrapInArraysAndConstIfNeeded(ColumnPtr res, const Columns & array_offsets, bool input_arg_is_const, size_t input_rows_count) const
    {
        for (auto it = array_offsets.rbegin(); it != array_offsets.rend(); ++it)
            res = ColumnArray::create(res, *it);

        if (input_arg_is_const)
            res = ColumnConst::create(res, input_rows_count);

        return res;
    }
};

}

REGISTER_FUNCTION(VariantElement)
{
    factory.registerFunction<FunctionVariantElement>(FunctionDocumentation{
        .description = R"(
Extracts a column with specified type from a `Variant` column.
)",
        .syntax{"variantElement(variant, type_name, [, default_value])"},
        .arguments{{
            {"variant", "Variant column"},
            {"type_name", "The name of the variant type to extract"},
            {"default_value", "The default value that will be used if variant doesn't have variant with specified type. Can be any type. Optional"}}},
        .examples{{{
            "Example",
            R"(
CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT v, variantElement(v, 'String'), variantElement(v, 'UInt64'), variantElement(v, 'Array(UInt64)') FROM test;)",
            R"(
┌─v─────────────┬─variantElement(v, 'String')─┬─variantElement(v, 'UInt64')─┬─variantElement(v, 'Array(UInt64)')─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ                        │                        ᴺᵁᴸᴸ │ []                                 │
│ 42            │ ᴺᵁᴸᴸ                        │                          42 │ []                                 │
│ Hello, World! │ Hello, World!               │                        ᴺᵁᴸᴸ │ []                                 │
│ [1,2,3]       │ ᴺᵁᴸᴸ                        │                        ᴺᵁᴸᴸ │ [1,2,3]                            │
└───────────────┴─────────────────────────────┴─────────────────────────────┴────────────────────────────────────┘
)"}}},
        .categories{"Variant"},
    });
}

}
