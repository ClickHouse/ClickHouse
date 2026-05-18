#include <Common/Exception.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>
#include <Functions/FunctionVariantAdaptor.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVariant.h>
#include <Interpreters/castColumn.h>

namespace DB
{


namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int TYPE_MISMATCH;
extern const int CANNOT_CONVERT_TYPE;
extern const int NO_COMMON_TYPE;
}

/// Strip LowCardinality wrapper from nested function result if present.
/// This is needed because the FunctionBaseVariantAdaptor constructor computes result types
/// using nullptr columns (treated as non-const by getReturnType), while executeImpl uses
/// actual ColumnConst (from scatter/filter of constant arguments). The difference in const-ness
/// changes the LowCardinality heuristic in getReturnType, potentially wrapping the result
/// in LowCardinality during execution but not during type computation.
static void removeLowCardinalityFromResult(DataTypePtr & result_type, ColumnPtr & result_column)
{
    if (typeid_cast<const DataTypeLowCardinality *>(result_type.get()))
    {
        result_type = removeLowCardinality(result_type);
        result_column = result_column->convertToFullColumnIfLowCardinality();
    }
}

ColumnPtr ExecutableFunctionVariantAdaptor::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t, bool dry_run) const
{
    auto column = arguments[variant_argument_index].column->convertToFullColumnIfConst();
    const auto & variant_column = assert_cast<const ColumnVariant &>(*column);
    if (variant_column.empty())
        return result_type->createColumn();

    const auto & variant_type = assert_cast<const DataTypeVariant &>(*arguments[variant_argument_index].type);
    const auto & variant_types = variant_type.getVariants();

    /// We use default implementation for Variant type only when default implementation for NULLs is used.
    /// If current column contains only NULLs, result column will also contain only NULLs.
    if (variant_column.hasOnlyNulls())
    {
        auto result = result_type->createColumn();
        result->insertManyDefaults(variant_column.size());
        return result;
    }

    /// Check if this Variant column contains only values of one type and no NULLs.
    /// In this case we can replace argument with this variant and execute the function without changing all other arguments.
    if (auto non_empty_variant_discr_no_nulls = variant_column.getGlobalDiscriminatorOfOneNoneEmptyVariantNoNulls())
    {
        /// Create new arguments and replace our Variant column with the single variant.
        auto global_discr = *non_empty_variant_discr_no_nulls;
        ColumnsWithTypeAndName new_arguments;
        new_arguments.reserve(arguments.size());
        for (size_t i = 0; i != arguments.size(); ++i)
        {
            if (i == variant_argument_index)
            {
                ColumnWithTypeAndName arg{
                    variant_column.getVariantPtrByGlobalDiscriminator(global_discr),
                    variant_types[global_discr],
                    arguments[i].name,
                };

                new_arguments.push_back(std::move(arg));
            }
            else
            {
                new_arguments.push_back(arguments[i]);
            }
        }

        /// Execute function on new arguments.
        auto func_base = function_overload_resolver->build(new_arguments);
        DataTypePtr nested_result_type = func_base->getResultType();
        ColumnPtr nested_result = func_base->execute(new_arguments, nested_result_type, variant_column.size(), dry_run);
        removeLowCardinalityFromResult(nested_result_type, nested_result);

        /// If result is Nullable(Nothing) or Nothing, just return column filled with NULLs/defaults.
        /// Nothing can appear when the function is executed on an empty type (e.g. arrayElement on Array(Nothing)).
        if (nested_result_type->onlyNull() || isNothing(nested_result_type))
        {
            auto res = result_type->createColumn();
            res->insertManyDefaults(variant_column.size());
            return res;
        }

        /// If the result of the function is not Variant, it means that this function returns the same
        /// type for all argument types (or similar types like FixedString or String).
        /// In this case we return Nullable of this type (because Variant can contain NULLs).
        if (!isVariant(result_type))
        {
            /// If return types are not the same, they must be convertible to each other (like FixedString/String).
            if (!removeNullable(result_type)->equals(*removeNullable(nested_result_type)))
            {
                try
                {
                    return castColumn(
                        ColumnWithTypeAndName{makeNullableSafe(nested_result), makeNullableSafe(nested_result_type), ""}, result_type);
                }
                catch (const Exception & e)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Cannot convert nested result of function {} with type {} to the expected result type {}: {}",
                        getName(),
                        removeNullable(result_type)->getName(),
                        removeNullable(nested_result_type)->getName(),
                        e.message());
                }
            }

            return makeNullableSafe(nested_result);
        }

        /// Result is Variant - use castColumn to handle the conversion.
        /// If nested result type is one of the variant types or a Variant type with a subset of resulting variants,
        /// castColumn will handle it correctly.
        try
        {
            return castColumn(ColumnWithTypeAndName{nested_result, nested_result_type, ""}, result_type);
        }
        catch (const Exception & e)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot convert nested result of function {} with type {} to the expected result type {}: {}",
                getName(),
                nested_result_type->getName(),
                result_type->getName(),
                e.message());
        }
    }

    /// Second, check if this Variant column contains only 1 variant and NULLs.
    /// In this case we can create a null-mask, filter all arguments by it and execute function
    /// on this variant and filtered arguments.
    if (auto non_empty_variant_discr = variant_column.getGlobalDiscriminatorOfOneNoneEmptyVariant())
    {
        auto global_discr = *non_empty_variant_discr;

        /// Create filter for rows containing our variant.
        PaddedPODArray<UInt8> filter;
        filter.reserve(variant_column.size());
        const auto & local_discriminators = variant_column.getLocalDiscriminators();
        auto local_discr = variant_column.localDiscriminatorByGlobal(global_discr);
        for (const auto & discr : local_discriminators)
            filter.push_back(discr == local_discr);

        /// Filter all other arguments using created filter.
        ColumnsWithTypeAndName new_arguments;
        new_arguments.reserve(arguments.size());
        size_t result_size_hint = variant_column.getVariantPtrByGlobalDiscriminator(global_discr)->size();
        for (size_t i = 0; i != arguments.size(); ++i)
        {
            if (i == variant_argument_index)
            {
                ColumnWithTypeAndName arg{
                    variant_column.getVariantPtrByGlobalDiscriminator(global_discr),
                    variant_types[global_discr],
                    arguments[i].name,
                };

                new_arguments.push_back(std::move(arg));
            }
            else
            {
                ColumnWithTypeAndName arg{arguments[i].column->filter(filter, result_size_hint), arguments[i].type, arguments[i].name};

                new_arguments.push_back(std::move(arg));
            }
        }

        /// Execute function on new arguments.
        auto func_base = function_overload_resolver->build(new_arguments);
        DataTypePtr nested_result_type = func_base->getResultType();
        ColumnPtr nested_result = func_base->execute(new_arguments, nested_result_type, new_arguments[0].column->size(), dry_run)
                            ->convertToFullColumnIfConst();
        removeLowCardinalityFromResult(nested_result_type, nested_result);

        /// If result is Nullable(Nothing) or Nothing, just return column filled with NULLs/defaults.
        if (nested_result_type->onlyNull() || isNothing(nested_result_type))
        {
            auto res = result_type->createColumn();
            res->insertManyDefaults(variant_column.size());
            return res;
        }

        /// If the result of the function is not Variant, it means that this function returns the same
        /// type for all argument types (or similar types like FixedString or String).
        /// In this case we return Nullable of this type (because Variant can contain NULLs).
        if (!isVariant(result_type))
        {
            /// Expand filtered result. If it's already Nullable, it will be filled with NULLs.
            nested_result->assumeMutable()->expand(filter, false);
            /// If result wasn't Nullable, create null-mask from filter and make it Nullable.
            if (!nested_result_type->isNullable() && nested_result_type->canBeInsideNullable())
            {
                for (auto & byte : filter)
                    byte = !byte;
                auto null_map_column = ColumnUInt8::create();
                null_map_column->getData() = std::move(filter);
                nested_result = ColumnNullable::create(nested_result, std::move(null_map_column));
                nested_result_type = makeNullable(nested_result_type);
            }

            /// If return types are not the same, they must be convertible to each other (like FixedString/String).
            if (!result_type->equals(*nested_result_type))
            {
                try
                {
                    return castColumn(ColumnWithTypeAndName{nested_result, nested_result_type, ""}, result_type);
                }
                catch (const Exception & e)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Cannot convert nested result of function {} with type {} to the expected result type {}: {}",
                        getName(),
                        result_type->getName(),
                        nested_result_type->getName(),
                        e.message());
                }
            }

            return nested_result;
        }

        /// If the result of nested function is Variant type, expand it and cast to result type
        /// The nested Variant may have a subset of types compared to the result Variant
        if (isVariant(nested_result_type))
        {
            nested_result->assumeMutable()->expand(filter, false);
            /// Cast to result type (handles case where nested Variant is a subset)
            try
            {
                return castColumn(ColumnWithTypeAndName{nested_result, nested_result_type, ""}, result_type);
            }
            catch (const Exception & e)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot convert nested result of function {} with type {} to the expected result type {}: {}",
                    getName(),
                    nested_result_type->getName(),
                    result_type->getName(),
                    e.message());
            }
        }

        /// If the result of nested function is not Variant, cast it to result Variant type and expand
        /// This handles both regular types and Nullable types automatically
        ColumnPtr result;
        try
        {
            result = castColumn(ColumnWithTypeAndName{nested_result, nested_result_type, ""}, result_type);
        }
        catch (const Exception & e)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot convert nested result of function {} with type {} to the expected result type {}: {}",
                getName(),
                nested_result_type->getName(),
                result_type->getName(),
                e.message());
        }

        /// Expand to match the original column size (filling filtered-out rows with NULLs)
        result->assumeMutable()->expand(filter, false);
        return result;
    }

    /// In general case with several variants we create a selector from discriminators
    /// and use it to create a set of filtered arguments for each variant.
    /// Then we will execute our function over all these arguments and construct the resulting column
    /// from all results based on created selector.
    const auto & local_discriminators = variant_column.getLocalDiscriminators();
    const auto & offsets = variant_column.getOffsets();
    size_t num_variants = variant_types.size();

    /// We use an array where index is the global discriminator.
    /// Variants that don't appear in the data will have null column pointers.
    /// Index num_variants is reserved for NULL values.
    std::vector<ColumnWithTypeAndName> variants;
    variants.resize(num_variants + 1);

    /// Create selector using global discriminators as indexes.
    /// Populate variants array only for discriminators that appear in the data.
    IColumn::Selector selector;
    selector.reserve(variant_column.size());
    for (char8_t local_discr : local_discriminators)
    {
        if (local_discr == ColumnVariant::NULL_DISCRIMINATOR)
        {
            selector.push_back(num_variants);
        }
        else
        {
            auto global_discr = variant_column.globalDiscriminatorByLocal(local_discr);
            /// Add this variant to the array if not already present.
            if (!variants[global_discr].column)
            {
                variants[global_discr] = ColumnWithTypeAndName{
                    variant_column.getVariantPtrByGlobalDiscriminator(global_discr),
                    variant_types[global_discr],
                    ""
                };
            }
            selector.push_back(global_discr);
        }
    }

    /// Create set of arguments for each variant using selector.
    std::vector<ColumnsWithTypeAndName> variants_arguments;
    variants_arguments.resize(variants.size());
    for (size_t i = 0; i != arguments.size(); ++i)
    {
        if (i == variant_argument_index)
        {
            /// Add variant arguments for variants that exist in the data (0 to num_variants-1).
            for (size_t j = 0; j < num_variants; ++j)
            {
                if (variants[j].column)
                    variants_arguments[j].push_back(variants[j]);
            }
        }
        else
        {
            auto columns = arguments[i].column->scatter(variants.size(), selector);
            for (size_t j = 0; j != variants_arguments.size(); ++j)
                variants_arguments[j].emplace_back(std::move(columns[j]), arguments[i].type, arguments[i].name);
        }
    }

    /// Execute function over all created sets of arguments and remember all results.
    std::vector<ColumnPtr> variants_results;
    std::vector<DataTypePtr> variants_result_types;
    variants_results.resize(variants.size());
    variants_result_types.resize(variants.size());
    /// Index num_variants is allocated for rows with NULL values, it doesn't have any result,
    /// we will insert NULL values in these rows.

    for (size_t i = 0; i < num_variants; ++i)
    {
        /// Skip variants that don't exist in the data.
        if (!variants[i].column)
            continue;

        auto func_base = function_overload_resolver->build(variants_arguments[i]);
        auto nested_result_type = func_base->getResultType();
        auto nested_result
            = func_base->execute(variants_arguments[i], nested_result_type, variants_arguments[i][0].column->size(), dry_run)
                  ->convertToFullColumnIfConst();
        removeLowCardinalityFromResult(nested_result_type, nested_result);

        variants_result_types[i] = nested_result_type;

        /// Set nullptr in case of only NULL or Nothing values, we will insert NULL for rows of this selector.
        if (nested_result_type->onlyNull() || isNothing(nested_result_type))
        {
            variants_results[i] = nullptr;
        }
        /// If the result of the function is not Variant, it means that this function returns the same
        /// type for all argument types (or similar types like FixedString or String).
        /// In this case we return Nullable of this type (because Variant can contain NULLs).
        else if (!isVariant(result_type))
        {
            /// If return types are not the same, they must be convertible to each other (like FixedString/String).
            if (!removeNullable(result_type)->equals(*removeNullable(nested_result_type)))
            {
                try
                {
                    variants_results[i] = castColumn(
                        ColumnWithTypeAndName{makeNullableSafe(nested_result), makeNullableSafe(nested_result_type), ""}, result_type);
                }
                catch (const Exception & e)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Cannot convert nested result of function {} with type {} to the expected result type {}: {}",
                        getName(),
                        result_type->getName(),
                        nested_result_type->getName(),
                        e.message());
                }
            }
            else
            {
                variants_results[i] = makeNullableSafe(nested_result);
            }
        }
        /// Result is Variant - keep the individual result columns, we'll build Variant manually
        else
        {
            variants_results[i] = nested_result;
        }
    }

    /// Construct resulting column
    if (!isVariant(result_type))
    {
        /// Non-Variant result: assemble from nullable results
        auto result = result_type->createColumn();
        result->reserve(variant_column.size());
        for (size_t i = 0; i != selector.size(); ++i)
        {
            if (selector[i] == num_variants || !variants_results[selector[i]])
                result->insertDefault();
            else
                result->insertFrom(*variants_results[selector[i]], offsets[i]);
        }
        return result;
    }

    /// Variant result: check if we can use optimized direct construction
    /// or need to use general approach with casting.
    const auto & result_variant_type = assert_cast<const DataTypeVariant &>(*result_type);

    /// Check if we can use optimized direct construction:
    /// 1. All result types must be different (no duplicates)
    /// 2. None of the result types should be a nested Variant
    /// 3. None of the result types should be Nullable or LowCardinality(Nullable)
    ///    (casting handles NULL extraction automatically)
    bool can_use_direct_construction = true;
    std::unordered_set<String> result_type_names;

    for (size_t i = 0; i < num_variants; ++i)
    {
        if (!variants[i].column || !variants_results[i])
            continue;

        const auto & variant_result_type = variants_result_types[i];

        /// Check if this result type is a Variant, Nullable, or LowCardinality(Nullable)
        /// Nullable results need casting to handle NULL values properly in the Variant
        /// This is done automatically during casts
        if (isVariant(variant_result_type) || variant_result_type->isNullable() || variant_result_type->isLowCardinalityNullable())
        {
            can_use_direct_construction = false;
            break;
        }

        /// Check for duplicate result types
        if (!result_type_names.insert(variant_result_type->getName()).second)
        {
            can_use_direct_construction = false;
            break;
        }
    }

    if (can_use_direct_construction)
    {
        /// Optimized path: build Variant directly from individual variant columns
        auto result = result_type->createColumn();
        auto & result_variant = assert_cast<ColumnVariant &>(*result);

        /// Map each variant result to its discriminator in the result Variant
        std::vector<std::optional<ColumnVariant::Discriminator>> result_discriminators(variants_results.size());

        for (size_t i = 0; i < num_variants; ++i)
        {
            /// Skip variants that don't exist in the data or have no result.
            if (!variants[i].column || !variants_results[i])
                continue;

            const auto & variant_result_type = variants_result_types[i];

            /// Find discriminator for this result type in the result Variant
            /// Remove Nullable wrapper since flattened types don't have it
            result_discriminators[i] = result_variant_type.tryGetVariantDiscriminator(removeNullable(variant_result_type)->getName());

            if (!result_discriminators[i])
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot find variant type {} in result Variant type {} during execution of {}",
                    removeNullable(variant_result_type)->getName(),
                    result_type->getName(),
                    getName());
        }

        /// Set variant columns in the result
        for (size_t i = 0; i < num_variants; ++i)
        {
            if (variants[i].column && variants_results[i] && result_discriminators[i])
            {
                auto global_discr = *result_discriminators[i];
                auto local_discr = result_variant.localDiscriminatorByGlobal(global_discr);
                result_variant.getVariantPtrByLocalDiscriminator(local_discr) = variants_results[i];
            }
        }

        /// Build discriminators and offsets
        auto & result_discriminators_col = result_variant.getLocalDiscriminators();
        auto & result_offsets = result_variant.getOffsets();
        result_discriminators_col.reserve(variant_column.size());
        result_offsets.reserve(variant_column.size());

        for (size_t i = 0; i != selector.size(); ++i)
        {
            if (selector[i] == num_variants || !variants_results[selector[i]])
            {
                result_discriminators_col.push_back(ColumnVariant::NULL_DISCRIMINATOR);
                result_offsets.emplace_back();
            }
            else
            {
                auto global_discr = *result_discriminators[selector[i]];
                auto local_discr = result_variant.localDiscriminatorByGlobal(global_discr);
                result_discriminators_col.push_back(local_discr);
                result_offsets.push_back(offsets[i]);
            }
        }

        return result;
    }
    /// General path: cast each result to final Variant type to handle
    /// duplicate result types and nested Variants correctly
    std::vector<ColumnPtr> casted_results(variants_results.size());

    for (size_t i = 0; i < num_variants; ++i)
    {
        if (!variants[i].column || !variants_results[i])
            continue;

        const auto & variant_result_type = variants_result_types[i];

        /// Cast this result to the final Variant type
        try
        {
            casted_results[i] = castColumn(ColumnWithTypeAndName{variants_results[i], variant_result_type, ""}, result_type);
        }
        catch (const Exception & e)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot convert nested result of function {} with type {} to the expected result type {}: {}",
                getName(),
                variant_result_type->getName(),
                result_type->getName(),
                e.message());
        }
    }

    /// Build result column row by row using selector and casted results
    auto result = result_type->createColumn();
    result->reserve(variant_column.size());

    for (size_t i = 0; i != selector.size(); ++i)
    {
        if (selector[i] == num_variants || !variants_results[selector[i]])
            result->insertDefault();
        else
            result->insertFrom(*casted_results[selector[i]], offsets[i]);
    }

    return result;
}

ColumnPtr ExecutableFunctionVariantAdaptor::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    return executeImpl(arguments, result_type, input_rows_count, false);
}

ColumnPtr ExecutableFunctionVariantAdaptor::executeDryRunImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    return executeImpl(arguments, result_type, input_rows_count, true);
}

FunctionBaseVariantAdaptor::FunctionBaseVariantAdaptor(
    std::shared_ptr<const IFunctionOverloadResolver> function_overload_resolver_,
    ColumnsWithTypeAndName arguments_with_type_)
    : function_overload_resolver(std::move(function_overload_resolver_))
{
    arguments.reserve(arguments_with_type_.size());
    for (const auto & arg : arguments_with_type_)
        arguments.push_back(arg.type);

    std::optional<size_t> first_variant_index;
    for (size_t i = 0; i != arguments.size(); ++i)
    {
        if (isVariant(arguments[i]))
        {
            first_variant_index = i;
            break;
        }
    }

    if (!first_variant_index.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No variant argument found for {}", function_overload_resolver->getName());

    variant_argument_index = *first_variant_index;

    /// Get the Variant argument type and its alternatives.
    const auto * variant_type = typeid_cast<const DataTypeVariant *>(arguments[variant_argument_index].get());
    if (!variant_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected Variant type at position {}", variant_argument_index);

    const auto & variant_alternatives = variant_type->getVariants();

    /// For each alternative in the Variant, build the function and get the actual result type.
    DataTypes result_types;
    result_types.reserve(variant_alternatives.size());

    for (const auto & alternative : variant_alternatives)
    {
        /// Create arguments with this alternative instead of the Variant.
        /// Preserve original columns (especially ColumnConst) for non-Variant arguments.
        ColumnsWithTypeAndName alt_columns_with_type = arguments_with_type_;
        alt_columns_with_type[variant_argument_index].type = alternative;
        /// Important: don't pass the original ColumnVariant with a non-Variant type
        alt_columns_with_type[variant_argument_index].column = nullptr;

        /// Get the return type for this alternative.
        /// Wrap in try-catch to handle incompatible type combinations gracefully.
        DataTypePtr alt_return_type;
        try
        {
            const auto func_base = function_overload_resolver->build(alt_columns_with_type);
            /// Strip LowCardinality from result type for consistency with executeImpl,
            /// where we also strip LC from nested function results.
            result_types.push_back(removeLowCardinality(func_base->getResultType()));
        }
        catch (const Exception & e)
        {
            /// If this combination of types is incompatible (e.g., Array(UInt32) vs UInt64),
            /// skip this alternative and treat it as if it doesn't participate in the result type.
            /// Only catch type-related errors - re-throw everything else.
            if (e.code() != ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT && e.code() != ErrorCodes::TYPE_MISMATCH
                && e.code() != ErrorCodes::CANNOT_CONVERT_TYPE && e.code() != ErrorCodes::NO_COMMON_TYPE)
                throw;
            /// Otherwise, skip this alternative
        }
    }

    /// If no valid result types were found, all alternatives are incompatible
    /// Return Nullable(Nothing) to indicate NULL result for all rows
    if (result_types.empty())
    {
        return_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
        return;
    }

    /// If all result types are the same (ignoring Nullable), return Nullable(common).
    /// Otherwise, return Variant(R0, R1, ...) in the same order.
    /// Compare by name to handle custom types correctly (like Geometry subtypes)
    bool all_same = true;
    DataTypePtr common_type = removeNullable(result_types[0]);

    for (size_t i = 1; i < result_types.size(); ++i)
    {
        DataTypePtr current_type = removeNullable(result_types[i]);
        if (common_type->getName() != current_type->getName())
        {
            all_same = false;
            break;
        }
    }

    if (all_same)
    {
        return_type = makeNullableSafe(common_type);
    }
    else
    {
        /// If we have Variant types in result_types, we need to flatten them
        /// because Variant inside Variant is not supported.
        /// We also need to remove Nullability from types because Nullable types are not allowed inside Variant.
        DataTypes flattened_types;
        for (const auto & type : result_types)
        {
            if (const auto * vt = typeid_cast<const DataTypeVariant *>(type.get()))
            {
                /// Extract inner variants and add them to flattened list
                const auto & inner_variants = vt->getVariants();
                flattened_types.insert(flattened_types.end(), inner_variants.begin(), inner_variants.end());
            }
            else
                flattened_types.push_back(removeNullable(type));
        }
        return_type = std::make_shared<DataTypeVariant>(flattened_types);
    }
}

}
