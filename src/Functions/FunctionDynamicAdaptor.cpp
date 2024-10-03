#include <Functions/FunctionDynamicAdaptor.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesBinaryEncoding.h>

#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnNullable.h>
#include <Interpreters/castColumn.h>
#include <IO/ReadBufferFromMemory.h>

namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ColumnPtr ExecutableFunctionDynamicAdaptor::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t, bool dry_run) const
{
    auto column = arguments[dynamic_argument_index].column->convertToFullColumnIfConst();
    const auto & dynamic_column = assert_cast<const ColumnDynamic &>(*column);
    if (dynamic_column.empty())
        return result_type->createColumn();

    const auto & variant_info = dynamic_column.getVariantInfo();
    const auto & variant_types = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants();
    const auto & variant_column = dynamic_column.getVariantColumn();
    auto shared_variant_discr = dynamic_column.getSharedVariantDiscriminator();

    /// We use default implementation for Dynamic type only when default implementation for NULLs is used.
    /// If current column contains only NULLs, result column will also contain only NULLs.
    if (variant_column.hasOnlyNulls())
    {
        auto result = result_type->createColumn();
        result->insertManyDefaults(variant_column.size());
        return result;
    }

    /// Check if this Dynamic column contains only values of one type and no NULLs.
    /// In this case we can replace argument with this variant and execute the function without changing all other arguments.
    auto non_empty_variant_discr_no_nulls = variant_column.getGlobalDiscriminatorOfOneNoneEmptyVariantNoNulls();
    if (non_empty_variant_discr_no_nulls && *non_empty_variant_discr_no_nulls != shared_variant_discr)
    {
        /// Crete new arguments and replace our Dynamic column with variant.
        auto global_discr = *non_empty_variant_discr_no_nulls;
        ColumnsWithTypeAndName new_arguments;
        new_arguments.reserve(arguments.size());
        for (size_t i = 0; i != arguments.size(); ++i)
        {
            if (i == dynamic_argument_index)
            {
                ColumnWithTypeAndName arg
                {
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
        auto nested_result = func_base->execute(new_arguments, nested_result_type, dynamic_column.size(), dry_run);

        /// If result is Nullable(Nothing), just return column filled with NULLs.
        if (nested_result_type->onlyNull())
        {
            auto res = result_type->createColumn();
            res->insertManyDefaults(dynamic_column.size());
            return res;
        }

        /// If the result of the function is not Dynamic, it means that this function returns the same
        /// type for all argument types (or similar types like FixedString or String).
        /// In this case we return Nullable of this type (because Dynamic can contain NULLs).
        if (!isDynamic(result_type))
        {
            /// If return types are not the same, they must be convertible to each other (like FixedString/String).
            if (!removeNullable(result_type)->equals(*removeNullable(nested_result_type)))
            {
                try
                {
                    return castColumn(ColumnWithTypeAndName{makeNullableSafe(nested_result), makeNullableSafe(nested_result_type), ""}, result_type);
                }
                catch (const Exception & e)
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot convert nested result of function {} with type {} to the expected result type {}: {}", getName(), removeNullable(result_type)->getName(), removeNullable(nested_result_type)->getName(), e.message());
                }
            }

            return makeNullableSafe(nested_result);
        }

        /// Cast column to result Dynamic type.
        return castColumn(ColumnWithTypeAndName{nested_result, nested_result_type, ""}, result_type);
    }

    /// Second, check if this Dynamic column contains only 1 variant and NULLs.
    /// In this case we can create a null-mask, filter all arguments by it and execute function
    /// on this variant and filtered arguments.
    auto non_empty_variant_discr = variant_column.getGlobalDiscriminatorOfOneNoneEmptyVariant();
    if (non_empty_variant_discr && *non_empty_variant_discr != shared_variant_discr)
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
            if (i == dynamic_argument_index)
            {
                ColumnWithTypeAndName arg
                {
                    variant_column.getVariantPtrByGlobalDiscriminator(global_discr),
                    variant_types[global_discr],
                    arguments[i].name,
                };

                new_arguments.push_back(std::move(arg));
            }
            else
            {
                ColumnWithTypeAndName arg
                {
                    arguments[i].column->filter(filter, result_size_hint),
                    arguments[i].type,
                    arguments[i].name
                };

                new_arguments.push_back(std::move(arg));
            }
        }

        /// Execute function on new arguments.
        auto func_base = function_overload_resolver->build(new_arguments);
        auto nested_result_type = func_base->getResultType();
        auto nested_result = func_base->execute(new_arguments, nested_result_type, new_arguments[0].column->size(), dry_run)->convertToFullColumnIfConst();

        /// If result is Nullable(Nothing), just return column filled with NULLs.
        if (nested_result_type->onlyNull())
        {
            auto res = result_type->createColumn();
            res->insertManyDefaults(dynamic_column.size());
            return res;
        }

        /// If the result of the function is not Dynamic, it means that this function returns the same
        /// type for all argument types (or similar types like FixedString or String).
        /// In this case we return Nullable of this type (because Dynamic can contain NULLs).
        if (!isDynamic(result_type))
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
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot convert nested result of function {} with type {} to the expected result type {}: {}", getName(), result_type->getName(), nested_result_type->getName(), e.message());
                }
            }

            return nested_result;
        }

        /// If the result of nested function is Variant type, we cannot use it as single variant inside Dynamic.
        /// In this case we cast the result to Dynamic result type.
        if (isVariant(nested_result_type))
        {
            nested_result = castColumn(ColumnWithTypeAndName{nested_result, nested_result_type, ""}, result_type);
            nested_result_type = result_type;
        }

        /// If the result of nested function is Dynamic (or we casted to it from Variant), we can just expand it
        /// and cast to the result Dynamic type (it can have different max_types parameter).
        if (isDynamic(nested_result_type))
        {
            nested_result->assumeMutable()->expand(filter, false);
            return castColumn(ColumnWithTypeAndName{nested_result, nested_result_type, ""}, result_type);
        }

        /// If the result of nested function is not Dynamic, we create Dynamic column with variant of this type.
        auto variant = nested_result;
        auto variant_type = nested_result_type;
        const NullMap * null_map_ptr = nullptr;
        /// If the result of nested function is Nullable, we create a null-mask and use it during Dynamic column creation,
        /// also the nested column inside Nullable will be filtered by this mask (inside Dynamic we don't store default values in rows with NULLs).
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
            variant_type = removeNullable(nested_result_type);
            null_map_ptr = &null_map;
        }

        auto result = result_type->createColumn();
        auto & result_dynamic = assert_cast<ColumnDynamic &>(*result);
        if (!result_dynamic.addNewVariant(variant_type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add new variant {} to Dynamic column {} during execution of {}", variant_type->getName(), result_type->getName(), getName());

        /// Now inside Dynamic we have empty Variant containing type of our column from function execution.
        /// Use our result column as variant and fill discriminators and offsets columns.
        auto & result_variant = result_dynamic.getVariantColumn();
        auto result_global_discr = result_dynamic.getVariantInfo().variant_name_to_discriminator.at(variant_type->getName());
        auto result_local_discr = result_variant.localDiscriminatorByGlobal(result_global_discr);
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
    /// We need to determine the selector index for rows with NULL values, but we don't know how many
    /// variants we have in Dynamic column (shared variant can contain unknown amount of new variant types).
    /// So, we allocate 0 index for rows with NULL values.
    variants.emplace_back();
    /// Remember indexes in selector for each variant type.
    std::unordered_map<String, size_t> variant_indexes;
    const auto & local_discriminators = variant_column.getLocalDiscriminators();
    const auto & offsets = variant_column.getOffsets();
    auto shared_variant_local_discr = variant_column.localDiscriminatorByGlobal(dynamic_column.getSharedVariantDiscriminator());
    const auto & shared_variant = dynamic_column.getSharedVariant();
    /// Remember created serializations for variants in shared variant to avoid recreating it every time.
    std::unordered_map<String, SerializationPtr> shared_variants_serializations;
    FormatSettings format_settings;
    for (size_t i = 0; i != local_discriminators.size(); ++i)
    {
        auto local_discr = local_discriminators[i];
        if (local_discr == ColumnVariant::NULL_DISCRIMINATOR)
        {
            selector.push_back(0);
            variants_offsets.emplace_back();
        }
        else if (local_discr == shared_variant_local_discr)
        {
            /// Deserialize type and value from shared variant row.
            auto value = shared_variant.getDataAt(offsets[i]);
            ReadBufferFromMemory buf(value.data, value.size);
            auto type = decodeDataType(buf);
            auto type_name = type->getName();

            /// Check if we already allocated selector index for this variant type.
            /// If not, append it to list of variants and remember its index.
            auto indexes_it = variant_indexes.find(type_name);
            if (indexes_it == variant_indexes.end())
            {
                indexes_it = variant_indexes.emplace(type_name, variants.size()).first;
                variants.emplace_back(type->createColumn(), type, "");
            }

            auto serializations_it = shared_variants_serializations.find(type_name);
            if (serializations_it == shared_variants_serializations.end())
                serializations_it = shared_variants_serializations.emplace(type_name, type->getDefaultSerialization()).first;

            /// Deserialize value into usual column.
            serializations_it->second->deserializeBinary(*variants[indexes_it->second].column->assumeMutable(), buf, format_settings);
            selector.push_back(indexes_it->second);
            variants_offsets.push_back(variants[indexes_it->second].column->size() - 1);
        }
        else
        {
            auto global_discr = variant_column.globalDiscriminatorByLocal(local_discr);
            /// Check if we already allocated selector index for this variant type.
            /// If not, append it to list of variants and remember its index.
            auto it = variant_indexes.find(variant_info.variant_names[global_discr]);
            if (it == variant_indexes.end())
            {
                it = variant_indexes.emplace(variant_info.variant_names[global_discr], variants.size()).first;
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
        if (i == dynamic_argument_index)
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
    variants_results.reserve(variants.size());
    /// 0 index is allocated for rows with NULL values, it doesn't have any result,
    /// we will insert NULL values in these rows.
    variants_results.emplace_back();
    for (size_t i = 1; i != variants_arguments.size(); ++i)
    {
        auto func_base = function_overload_resolver->build(variants_arguments[i]);
        auto nested_result_type = func_base->getResultType();
        auto nested_result = func_base->execute(variants_arguments[i], nested_result_type, variants_arguments[i][0].column->size(), dry_run)->convertToFullColumnIfConst();

        /// Append nullptr in case of only NULL values, we will insert NULL for rows of this selector.
        if (nested_result_type->onlyNull())
        {
            variants_results.emplace_back();
        }
        /// If the result of the function is not Dynamic, it means that this function returns the same
        /// type for all argument types (or similar types like FixedString or String).
        /// In this case we return Nullable of this type (because Dynamic can contain NULLs).
        else if (!isDynamic(result_type))
        {
            /// If return types are not the same, they must be convertible to each other (like FixedString/String).
            if (!removeNullable(result_type)->equals(*removeNullable(nested_result_type)))
            {
                try
                {
                    variants_results.push_back(castColumn(ColumnWithTypeAndName{makeNullableSafe(nested_result), makeNullableSafe(nested_result_type), ""}, result_type));
                }
                catch (const Exception & e)
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot convert nested result of function {} with type {} to the expected result type {}: {}", getName(), result_type->getName(), nested_result_type->getName(), e.message());
                }
            }
            else
            {
                variants_results.push_back(makeNullableSafe(nested_result));
            }
        }
        /// Otherwise cast this result to the resulting Dynamic type.
        else
        {
            variants_results.push_back(castColumn(ColumnWithTypeAndName{nested_result, nested_result_type, ""}, result_type));
        }
    }

    /// Construct resulting Dynamic column from all results.
    auto result = result_type->createColumn();
    result->reserve(dynamic_column.size());
    for (size_t i = 0; i != selector.size(); ++i)
    {
        if (selector[i] == 0 || !variants_results[selector[i]])
            result->insertDefault();
        else
            result->insertFrom(*variants_results[selector[i]], variants_offsets[i]);
    }

    return result;
}

ColumnPtr ExecutableFunctionDynamicAdaptor::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    return executeImpl(arguments, result_type, input_rows_count, false);
}

ColumnPtr ExecutableFunctionDynamicAdaptor::executeDryRunImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    return executeImpl(arguments, result_type, input_rows_count, true);
}

FunctionBaseDynamicAdaptor::FunctionBaseDynamicAdaptor(std::shared_ptr<const IFunctionOverloadResolver> function_overload_resolver_, DataTypes arguments_) : function_overload_resolver(function_overload_resolver_), arguments(arguments_)
{
    /// For resulting Dynamic type use the maximum max_dynamic_types from all Dynamic arguments.
    size_t result_max_dynamic_type;
    bool first = true;
    for (size_t i = 0; i != arguments.size(); ++i)
    {
        if (const auto * dynamic_type = typeid_cast<const DataTypeDynamic *>(arguments[i].get()))
        {
            if (first)
            {
                result_max_dynamic_type = dynamic_type->getMaxDynamicTypes();
                dynamic_argument_index = i;
                first = false;
            }
            else
            {
                result_max_dynamic_type = std::max(result_max_dynamic_type, dynamic_type->getMaxDynamicTypes());
            }
        }
    }

    if (auto type = function_overload_resolver->getReturnTypeForDefaultImplementationForDynamic())
        return_type = makeNullableSafe(type);
    else
        return_type = std::make_shared<DataTypeDynamic>(result_max_dynamic_type);
}

}
