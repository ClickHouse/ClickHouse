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
        auto nested_result_type = func_base->getResultType();
        auto nested_result = func_base->execute(new_arguments, nested_result_type, variant_column.size(), dry_run);

        /// If result is Nullable(Nothing), just return column filled with NULLs.
        if (nested_result_type->onlyNull())
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

        /// Result is Variant - build the result Variant column directly
        /// Find which discriminator this result type corresponds to
        auto result_column = result_type->createColumn();
        auto & result_variant = assert_cast<ColumnVariant &>(*result_column);
        const auto & result_variant_type = assert_cast<const DataTypeVariant &>(*result_type);
        const auto & result_variants = result_variant_type.getVariants();

        /// Find discriminator by name first (for custom types), then by equals()
        std::optional<ColumnVariant::Discriminator> result_global_discr;
        for (size_t i = 0; i < result_variants.size(); ++i)
        {
            if (result_variants[i]->getName() == nested_result_type->getName())
            {
                result_global_discr = i;
                break;
            }
        }
        if (!result_global_discr)
        {
            for (size_t i = 0; i < result_variants.size(); ++i)
            {
                if (result_variants[i]->equals(*nested_result_type))
                {
                    result_global_discr = i;
                    break;
                }
            }
        }

        if (!result_global_discr)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot find variant type {} in result Variant type {} during execution of {}",
                nested_result_type->getName(),
                result_type->getName(),
                getName());

        /// Set up the variant column with all rows pointing to this single variant
        auto result_local_discr = result_variant.localDiscriminatorByGlobal(*result_global_discr);
        result_variant.getVariantPtrByLocalDiscriminator(result_local_discr) = nested_result;

        auto & discriminators = result_variant.getLocalDiscriminators();
        auto & offsets = result_variant.getOffsets();
        discriminators.reserve(variant_column.size());
        offsets.reserve(variant_column.size());

        for (size_t i = 0; i < variant_column.size(); ++i)
        {
            discriminators.push_back(result_local_discr);
            offsets.push_back(i);
        }

        return result_column;
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
                ColumnWithTypeAndName arg{
                    arguments[i].column->convertToFullColumnIfConst()->filter(filter, result_size_hint),
                    arguments[i].type,
                    arguments[i].name};

                new_arguments.push_back(std::move(arg));
            }
        }

        /// Execute function on new arguments.
        auto func_base = function_overload_resolver->build(new_arguments);
        auto nested_result_type = func_base->getResultType();
        auto nested_result
            = func_base->execute(new_arguments, nested_result_type, new_arguments[0].column->size(), dry_run)->convertToFullColumnIfConst();

        /// If result is Nullable(Nothing), just return column filled with NULLs.
        if (nested_result_type->onlyNull())
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

        /// If the result of nested function is Variant type, we need to expand it
        if (isVariant(nested_result_type))
        {
            nested_result->assumeMutable()->expand(filter, false);
            /// Result is already the right Variant type (we use input type as return type)
            return nested_result;
        }

        /// If the result of nested function is not Variant, we create Variant column with this type as one of the variants.
        auto variant = nested_result;
        auto variant_type_for_result = nested_result_type;
        const NullMap * null_map_ptr = nullptr;
        /// If the result of nested function is Nullable, we create a null-mask and use it during Variant column creation,
        /// also the nested column inside Nullable will be filtered by this mask (inside Variant we don't store default values in rows with NULLs).
        if (const auto & column_nullable = typeid_cast<const ColumnNullable *>(variant.get()))
        {
            const auto & null_map = column_nullable->getNullMapData();
            /// Create filter for nested column from null-map and calculate result size hint.
            PaddedPODArray<UInt8> nested_filter;
            nested_filter.reserve(null_map.size());
            size_t size_hint = 0;

            for (auto byte : null_map)
            {
                if (byte)
                {
                    nested_filter.push_back(0);
                }
                else
                {
                    nested_filter.push_back(1);
                    ++size_hint;
                }
            }

            variant = column_nullable->getNestedColumnPtr()->filter(nested_filter, size_hint);
            variant_type_for_result = removeNullable(nested_result_type);
            null_map_ptr = &null_map;
        }

        auto result = result_type->createColumn();
        auto & result_variant = assert_cast<ColumnVariant &>(*result);
        const auto & result_variant_type = assert_cast<const DataTypeVariant &>(*result_type);

        /// Find the discriminator in result Variant type that matches our variant type.
        /// For custom-named types (like Geometry's Point, Polygon, etc.), we must match by name
        /// because equals() would match types with the same underlying structure (e.g., Polygon and MultiLineString).
        std::optional<ColumnVariant::Discriminator> result_global_discr;
        const auto & result_variants = result_variant_type.getVariants();

        /// First try to match by name (for custom types)
        for (size_t i = 0; i < result_variants.size(); ++i)
        {
            if (result_variants[i]->getName() == variant_type_for_result->getName())
            {
                result_global_discr = i;
                break;
            }
        }

        /// If not found by name, try equals() (for regular types without custom names)
        if (!result_global_discr)
        {
            for (size_t i = 0; i < result_variants.size(); ++i)
            {
                if (result_variants[i]->equals(*variant_type_for_result))
                {
                    result_global_discr = i;
                    break;
                }
            }
        }

        if (!result_global_discr)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot find variant type {} in result Variant type {} during execution of {}",
                variant_type_for_result->getName(),
                result_type->getName(),
                getName());

        /// Now inside Variant we have the correct variant type.
        /// Use our result column as variant and fill discriminators and offsets columns.
        auto result_local_discr = result_variant.localDiscriminatorByGlobal(*result_global_discr);
        result_variant.getVariantPtrByLocalDiscriminator(result_local_discr) = std::move(variant);

        auto & result_local_discriminators = result_variant.getLocalDiscriminators();
        result_local_discriminators.reserve(filter.size());
        auto & result_offsets = result_variant.getOffsets();
        result_offsets.reserve(filter.size());
        /// Calculate correct offset for our variant, we cannot use initial offsets from
        /// argument column because we could filter result column by its null-map.
        size_t offset = 0;
        /// Use initial offsets from argument column to use correct values of null-map.
        const auto & offsets = variant_column.getOffsets();
        for (size_t i = 0; i != filter.size(); ++i)
        {
            if (filter[i] && (!null_map_ptr || !(*null_map_ptr)[offsets[i]]))
            {
                result_local_discriminators.push_back(result_local_discr);
                result_offsets.push_back(offset++);
            }
            else
            {
                result_local_discriminators.push_back(ColumnVariant::NULL_DISCRIMINATOR);
                result_offsets.emplace_back();
            }
        }

        return result;
    }

    /// In general case with several variants we create a selector from discriminators
    /// and use it to create a set of filtered arguments for each variant.
    /// Then we will execute our function over all these arguments and construct the resulting column
    /// from all results based on created selector.
    IColumn::Selector selector;
    selector.reserve(variant_column.size());
    IColumn::Offsets variants_offsets;
    variants_offsets.reserve(variant_column.size());
    std::vector<ColumnWithTypeAndName> variants;
    /// We need to determine the selector index for rows with NULL values.
    /// We allocate 0 index for rows with NULL values.
    variants.emplace_back();
    /// Remember indexes in selector for each variant type.
    std::unordered_map<ColumnVariant::Discriminator, size_t> variant_indexes;
    const auto & local_discriminators = variant_column.getLocalDiscriminators();
    const auto & offsets = variant_column.getOffsets();

    for (size_t i = 0; i != local_discriminators.size(); ++i)
    {
        auto local_discr = local_discriminators[i];
        if (local_discr == ColumnVariant::NULL_DISCRIMINATOR)
        {
            selector.push_back(0);
            variants_offsets.emplace_back();
        }
        else
        {
            auto global_discr = variant_column.globalDiscriminatorByLocal(local_discr);
            /// Check if we already allocated selector index for this variant type.
            /// If not, append it to list of variants and remember its index.
            auto it = variant_indexes.find(global_discr);
            if (it == variant_indexes.end())
            {
                it = variant_indexes.emplace(global_discr, variants.size()).first;
                variants.emplace_back(variant_column.getVariantPtrByLocalDiscriminator(local_discr), variant_types[global_discr], "");
            }

            selector.push_back(it->second);
            variants_offsets.push_back(offsets[i]);
        }
    }

    /// Create set of arguments for each variant using selector.
    std::vector<ColumnsWithTypeAndName> variants_arguments;
    variants_arguments.resize(variants.size());
    for (size_t i = 0; i != arguments.size(); ++i)
    {
        if (i == variant_argument_index)
        {
            for (size_t j = 1; j != variants_arguments.size(); ++j)
                variants_arguments[j].push_back(variants[j]);
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
    variants_results.reserve(variants.size());
    variants_result_types.reserve(variants.size());
    /// 0 index is allocated for rows with NULL values, it doesn't have any result,
    /// we will insert NULL values in these rows.
    variants_results.emplace_back();
    variants_result_types.emplace_back();

    for (size_t i = 1; i != variants_arguments.size(); ++i)
    {
        auto func_base = function_overload_resolver->build(variants_arguments[i]);
        auto nested_result_type = func_base->getResultType();
        auto nested_result = func_base->execute(variants_arguments[i], nested_result_type, variants_arguments[i][0].column->size(), dry_run)
                                 ->convertToFullColumnIfConst();

        variants_result_types.push_back(nested_result_type);

        /// Append nullptr in case of only NULL values, we will insert NULL for rows of this selector.
        if (nested_result_type->onlyNull())
        {
            variants_results.emplace_back();
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
                    variants_results.push_back(castColumn(
                        ColumnWithTypeAndName{makeNullableSafe(nested_result), makeNullableSafe(nested_result_type), ""}, result_type));
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
                variants_results.push_back(makeNullableSafe(nested_result));
            }
        }
        /// Result is Variant - keep the individual result columns, we'll build Variant manually
        else
        {
            variants_results.push_back(nested_result);
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
            if (selector[i] == 0 || !variants_results[selector[i]])
                result->insertDefault();
            else
                result->insertFrom(*variants_results[selector[i]], variants_offsets[i]);
        }
        return result;
    }

    /// Variant result: build Variant column directly from individual variant columns
    auto result = result_type->createColumn();
    auto & result_variant = assert_cast<ColumnVariant &>(*result);
    const auto & result_variant_type = assert_cast<const DataTypeVariant &>(*result_type);
    const auto & result_variants = result_variant_type.getVariants();

    /// Map each variant result to its discriminator in the result Variant
    std::vector<std::optional<ColumnVariant::Discriminator>> result_discriminators(variants_results.size());

    for (size_t i = 1; i < variants_results.size(); ++i)
    {
        if (!variants_results[i])
            continue;

        const auto & variant_result_type = variants_result_types[i];

        /// Find discriminator by name first (for custom types), then by equals()
        for (size_t j = 0; j < result_variants.size(); ++j)
        {
            if (result_variants[j]->getName() == variant_result_type->getName())
            {
                result_discriminators[i] = j;
                break;
            }
        }
        if (!result_discriminators[i])
        {
            for (size_t j = 0; j < result_variants.size(); ++j)
            {
                if (result_variants[j]->equals(*variant_result_type))
                {
                    result_discriminators[i] = j;
                    break;
                }
            }
        }

        if (!result_discriminators[i])
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot find variant type {} in result Variant type {} during execution of {}",
                variant_result_type->getName(),
                result_type->getName(),
                getName());
    }

    /// Set variant columns in the result
    for (size_t i = 1; i < variants_results.size(); ++i)
    {
        if (variants_results[i] && result_discriminators[i])
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
        if (selector[i] == 0 || !variants_results[selector[i]])
        {
            result_discriminators_col.push_back(ColumnVariant::NULL_DISCRIMINATOR);
            result_offsets.emplace_back();
        }
        else
        {
            auto global_discr = *result_discriminators[selector[i]];
            auto local_discr = result_variant.localDiscriminatorByGlobal(global_discr);
            result_discriminators_col.push_back(local_discr);
            result_offsets.push_back(variants_offsets[i]);
        }
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
    std::shared_ptr<const IFunctionOverloadResolver> function_overload_resolver_, DataTypes arguments_)
    : function_overload_resolver(function_overload_resolver_)
    , arguments(arguments_)
{
    std::optional<size_t> first_variant_index;
    for (size_t i = 0; i != arguments.size(); ++i)
    {
        if (isVariant(arguments[i]))
        {
            if (!first_variant_index.has_value())
            {
                first_variant_index = i;
                break;
            }
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
        DataTypes alt_arguments = arguments;
        alt_arguments[variant_argument_index] = alternative;

        /// Build the function for this alternative.
        ColumnsWithTypeAndName alt_columns_with_type;
        alt_columns_with_type.reserve(alt_arguments.size());
        for (const auto & arg : alt_arguments)
            alt_columns_with_type.push_back({nullptr, arg, ""});

        /// Get the return type for this alternative.
        DataTypePtr alt_return_type = function_overload_resolver->getReturnType(alt_columns_with_type);
        result_types.push_back(alt_return_type);
    }

    /// If all result types are the same (ignoring Nullable), return Nullable(common).
    /// Otherwise, return Variant(R0, R1, ...) in the same order.
    bool all_same = true;
    DataTypePtr common_type = removeNullable(result_types[0]);

    for (size_t i = 1; i < result_types.size(); ++i)
    {
        DataTypePtr current_type = removeNullable(result_types[i]);
        if (!common_type->equals(*current_type))
        {
            all_same = false;
            break;
        }
    }

    if (all_same)
        return_type = makeNullableSafe(common_type);
    else
        return_type = std::make_shared<DataTypeVariant>(result_types);
}

}
