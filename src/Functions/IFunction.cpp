#include <Functions/FunctionDynamicAdaptor.h>
#include <Functions/FunctionVariantAdaptor.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnsCommon.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Native.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include "config.h"

#include <cstdlib>
#include <memory>

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#endif

namespace ProfileEvents
{
    extern const Event DefaultImplementationForNullsRows;
    extern const Event DefaultImplementationForNullsRowsWithNulls;
}

namespace DB
{

namespace Setting
{
extern const SettingsBool short_circuit_function_evaluation_for_nulls;
extern const SettingsDouble short_circuit_function_evaluation_for_nulls_threshold;
extern const SettingsBool use_variant_default_implementation_for_comparisons;
}

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

bool allArgumentsAreConstants(const ColumnsWithTypeAndName & args)
{
    for (const auto & arg : args)
        if (!isColumnConst(*arg.column))
            return false;
    return true;
}

ColumnPtr replaceLowCardinalityColumnsByNestedAndGetDictionaryIndexes(
    ColumnsWithTypeAndName & args, bool can_be_executed_on_default_arguments, size_t input_rows_count)
{
    size_t num_rows = input_rows_count;
    ColumnPtr indexes;

    /// Find first LowCardinality column and replace it with nested dictionary.
    for (auto & column : args)
    {
        if (const auto * low_cardinality_column = checkAndGetColumn<ColumnLowCardinality>(column.column.get()))
        {
            /// Only a single LowCardinality column is supported now.
            if (indexes)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Default functions implementation for LowCardinality is supported only with a single LowCardinality argument.");

            const auto * low_cardinality_type = checkAndGetDataType<DataTypeLowCardinality>(column.type.get());

            if (!low_cardinality_type)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Incompatible type for LowCardinality column: {}", column.type->getName());

            if (can_be_executed_on_default_arguments)
            {
                /// Normal case, when function can be executed on values' default.
                column.column = low_cardinality_column->getDictionary().getNestedColumn();
                indexes = low_cardinality_column->getIndexesPtr();
            }
            else
            {
                /// Special case when default value can't be used. Example: 1 % LowCardinality(Int).
                /// LowCardinality always contains default, so 1 % 0 will throw exception in normal case.
                auto dict_encoded = low_cardinality_column->getMinimalDictionaryEncodedColumn(0, low_cardinality_column->size());
                column.column = dict_encoded.dictionary;
                indexes = dict_encoded.indexes;
            }

            num_rows = column.column->size();
            column.type = low_cardinality_type->getDictionaryType();
        }
    }

    /// Change size of constants.
    for (auto & column : args)
    {
        if (const auto * column_const = checkAndGetColumn<ColumnConst>(column.column.get()))
        {
            column.column = ColumnConst::create(recursiveRemoveLowCardinality(column_const->getDataColumnPtr()), num_rows);
            column.type = recursiveRemoveLowCardinality(column.type);
        }
    }

    return indexes;
}

void convertLowCardinalityColumnsToFull(ColumnsWithTypeAndName & args)
{
    for (auto & column : args)
    {
        column.column = recursiveRemoveLowCardinality(column.column);
        column.type = recursiveRemoveLowCardinality(column.type);
    }
}
}

ColumnPtr IExecutableFunction::defaultImplementationForConstantArguments(
    const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const
{
    ColumnNumbers arguments_to_remain_constants = getArgumentsThatAreAlwaysConstant();

    /// Check that these arguments are really constant.
    for (auto arg_num : arguments_to_remain_constants)
        if (arg_num < args.size() && !isColumnConst(*args[arg_num].column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument at index {} for function {} must be constant", arg_num, getName());

    if (args.empty() || !useDefaultImplementationForConstants() || !allArgumentsAreConstants(args))
        return nullptr;

    ColumnsWithTypeAndName temporary_columns;
    bool have_converted_columns = false;

    size_t arguments_size = args.size();
    temporary_columns.reserve(arguments_size);
    for (size_t arg_num = 0; arg_num < arguments_size; ++arg_num)
    {
        const ColumnWithTypeAndName & column = args[arg_num];

        if (arguments_to_remain_constants.end()
            != std::find(arguments_to_remain_constants.begin(), arguments_to_remain_constants.end(), arg_num))
        {
            temporary_columns.emplace_back(ColumnWithTypeAndName{column.column->cloneResized(1), column.type, column.name});
        }
        else
        {
            have_converted_columns = true;
            temporary_columns.emplace_back(
                ColumnWithTypeAndName{assert_cast<const ColumnConst *>(column.column.get())->getDataColumnPtr(), column.type, column.name});
        }
    }

    /** When using default implementation for constants, the function requires at least one argument
      *  not in "arguments_to_remain_constants" set. Otherwise we get infinite recursion.
      */
    if (!have_converted_columns)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: the function requires more arguments",
            getName());

    ColumnPtr result_column = executeWithoutLowCardinalityColumns(temporary_columns, result_type, 1, dry_run);

    /// extremely rare case, when we have function with completely const arguments
    /// but some of them produced by non isDeterministic function
    if (result_column->size() > 1)
        result_column = result_column->cloneResized(1);

    return ColumnConst::create(result_column, input_rows_count);
}


ColumnPtr IExecutableFunction::defaultImplementationForNulls(
    const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const
{
    if (args.empty() || !useDefaultImplementationForNulls())
        return nullptr;

    NullPresence null_presence = getNullPresense(args);

    if (null_presence.has_null_constant)
    {
        // Default implementation for nulls returns null result for null arguments,
        // so the result type must be nullable.
        if (!result_type->isNullable())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Function {} with Null argument and default implementation for Nulls "
                "is expected to return Nullable result, got {}",
                getName(),
                result_type->getName());

        /// If any of the input arguments is null literal, the result is null constant.
        return result_type->createColumnConstWithDefaultValue(input_rows_count);
    }

    if (null_presence.has_nullable)
    {
        if (input_rows_count == 0)
        {
            /// We are not sure if it is const column or not if has_nullable is true.
            return result_type->createColumn();
        }

        bool all_columns_constant = true;
        bool all_numeric_types = true;
        for (const auto & arg: args)
        {
            if (!isColumnConst(*arg.column))
                all_columns_constant = false;

            if (arg.type->isNullable() && isColumnConst(*arg.column) && arg.column->onlyNull())
            {
                /// If any of input columns contains a null constant, the result is null constant.
                return result_type->createColumnConstWithDefaultValue(input_rows_count);
            }

            WhichDataType which(removeNullable(arg.type));
            if (!which.isNumber() && !which.isEnum() && !which.isDateOrDate32OrDateTimeOrDateTime64() && !which.isInterval())
                all_numeric_types = false;
        }

        if (all_columns_constant || all_numeric_types)
        {
            /// When all columns are constant or numeric, the cost of [[countBytesInFilter]] or [[ColumnUInt8::create]] should not be ignored.
            /// That's why we add a fast path for this case.
            ColumnsWithTypeAndName temporary_columns = createBlockWithNestedColumns(args);
            auto temporary_result_type = removeNullable(result_type);

            auto res = executeWithoutLowCardinalityColumns(temporary_columns, temporary_result_type, input_rows_count, dry_run);
            return wrapInNullable(res, args, result_type, input_rows_count);
        }

        ColumnPtr result_null_map = ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0));
        for (const auto & arg : args)
        {
            if (arg.type->isNullable() && !isColumnConst(*arg.column))
            {
                if (result_null_map)
                {
                    MutableColumnPtr mut = IColumn::mutate(std::move(result_null_map));
                    auto & result_null_map_data = assert_cast<ColumnUInt8 &>(*mut).getData();
                    const auto & null_map = assert_cast<const ColumnNullable &>(*arg.column).getNullMapData();
                    for (size_t i = 0; i < input_rows_count; ++i)
                        result_null_map_data[i] |= null_map[i];
                    result_null_map = std::move(mut);
                }
                else
                {
                    /// If only one arg is nullable, share its null map between the arg and the result.
                    result_null_map = assert_cast<const ColumnNullable &>(*arg.column).getNullMapColumnPtr();
                }
            }
        }

        size_t rows_with_nulls = result_null_map ?
            countBytesInFilter(assert_cast<const ColumnUInt8 &>(*result_null_map).getData().data(),
                               0, input_rows_count) : 0;
        size_t rows_without_nulls = input_rows_count - rows_with_nulls;
        ProfileEvents::increment(ProfileEvents::DefaultImplementationForNullsRows, input_rows_count);
        ProfileEvents::increment(ProfileEvents::DefaultImplementationForNullsRowsWithNulls, rows_with_nulls);

        if (rows_without_nulls == 0)
        {
            /// Don't need to evaluate function if each row contains at least one null value and not all input columns are constant.
            return result_type->createColumnConstWithDefaultValue(input_rows_count)->convertToFullColumnIfConst();
        }

        double null_ratio = static_cast<double>(rows_with_nulls) / static_cast<double>(input_rows_count);
        bool should_short_circuit
            = short_circuit_function_evaluation_for_nulls && null_ratio >= short_circuit_function_evaluation_for_nulls_threshold;

        ColumnsWithTypeAndName temporary_columns = createBlockWithNestedColumns(args);
        auto temporary_result_type = removeNullable(result_type);

        if (!should_short_circuit)
        {
            /// Each row should be evaluated if there are no nulls or short circuiting is disabled.
            auto res = executeWithoutLowCardinalityColumns(temporary_columns, temporary_result_type, input_rows_count, dry_run);
            auto new_res = wrapInNullable(res, std::move(result_null_map));
            return new_res;
        }
        else
        {
            /// If short circuit is enabled, we only execute the function on rows with all arguments not null

            /// Generate Filter
            IColumn::Filter filter_mask(input_rows_count);
            const auto & result_null_map_data = assert_cast<const ColumnUInt8 &>(*result_null_map).getData();
            for (size_t i = 0; i < input_rows_count; ++i)
                filter_mask[i] = !result_null_map_data[i];

            /// Filter every column by mask
            for (auto & col : temporary_columns)
                col.column = col.column->filter(filter_mask, rows_without_nulls);

            auto res = executeWithoutLowCardinalityColumns(temporary_columns, temporary_result_type, rows_without_nulls, dry_run);
            auto mutable_res = IColumn::mutate(std::move(res));
            mutable_res->expand(filter_mask, false);

            auto new_res = wrapInNullable(std::move(mutable_res), std::move(result_null_map));
            return new_res;
        }
    }

    return nullptr;
}

ColumnPtr IExecutableFunction::defaultImplementationForNothing(
    const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count) const
{
    if (!useDefaultImplementationForNothing())
        return nullptr;

    bool is_nothing_type_presented = false;
    for (const auto & arg : args)
        is_nothing_type_presented |= isNothing(arg.type);

    if (!is_nothing_type_presented)
        return nullptr;

    if (!isNothing(result_type))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Function {} with argument with type Nothing and default implementation for Nothing "
            "is expected to return result with type Nothing, got {}",
            getName(),
            result_type->getName());

    if (input_rows_count > 0)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot create non-empty column with type Nothing");
    return ColumnNothing::create(0);
}

ColumnPtr IExecutableFunction::executeWithoutLowCardinalityColumns(
    const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const
{
    if (auto res = defaultImplementationForNothing(args, result_type, input_rows_count))
        return res;

    if (auto res = defaultImplementationForConstantArguments(args, result_type, input_rows_count, dry_run))
        return res;

    if (auto res = defaultImplementationForNulls(args, result_type, input_rows_count, dry_run))
        return res;

    ColumnPtr res;
    if (dry_run)
        res = executeDryRunImpl(args, result_type, input_rows_count);
    else
        res = executeImpl(args, result_type, input_rows_count);

    if (!res)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty column was returned by function {}", getName());

    return res;
}

static void convertSparseColumnsToFull(ColumnsWithTypeAndName & args)
{
    for (auto & column : args)
        column.column = recursiveRemoveSparse(column.column);
}

static void convertReplicatedColumnsToFull(ColumnsWithTypeAndName & args)
{
    for (auto & column : args)
        column.column = column.column->convertToFullColumnIfReplicated();
}

IExecutableFunction::IExecutableFunction()
{
    if (CurrentThread::isInitialized())
    {
        auto query_context = CurrentThread::get().getQueryContext();
        if (query_context && query_context->getSettingsRef()[Setting::short_circuit_function_evaluation_for_nulls])
        {
            short_circuit_function_evaluation_for_nulls = true;
            short_circuit_function_evaluation_for_nulls_threshold = query_context->getSettingsRef()[Setting::short_circuit_function_evaluation_for_nulls_threshold];
        }
    }
}

ColumnPtr IExecutableFunction::executeWithoutSparseColumns(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const
{
    ColumnPtr result;
    if (useDefaultImplementationForLowCardinalityColumns())
    {
        ColumnsWithTypeAndName columns_without_low_cardinality = arguments;

        if (const auto * res_low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(result_type.get()))
        {
            bool can_be_executed_on_default_arguments = canBeExecutedOnDefaultArguments();

            const auto & dictionary_type = res_low_cardinality_type->getDictionaryType();
            ColumnPtr indexes = replaceLowCardinalityColumnsByNestedAndGetDictionaryIndexes(
                columns_without_low_cardinality, can_be_executed_on_default_arguments, input_rows_count);

            size_t new_input_rows_count
                = columns_without_low_cardinality.empty() ? input_rows_count : columns_without_low_cardinality.front().column->size();
            checkFunctionArgumentSizes(columns_without_low_cardinality, new_input_rows_count);

            auto res = executeWithoutLowCardinalityColumns(columns_without_low_cardinality, dictionary_type, new_input_rows_count, dry_run);
            bool res_is_constant = isColumnConst(*res);

            auto keys = res_is_constant ? res->cloneResized(1)->convertToFullColumnIfConst() : res;

            auto res_mut_dictionary = DataTypeLowCardinality::createColumnUnique(*res_low_cardinality_type->getDictionaryType());
            ColumnPtr res_indexes = res_mut_dictionary->uniqueInsertRangeFrom(*keys, 0, keys->size());
            ColumnUniquePtr res_dictionary = std::move(res_mut_dictionary);

            if (indexes && !res_is_constant)
                result = ColumnLowCardinality::create(res_dictionary, res_indexes->index(*indexes, 0), /*is_shared=*/false);
            else
                result = ColumnLowCardinality::create(res_dictionary, res_indexes, /*is_shared=*/false);

            if (res_is_constant)
                result = ColumnConst::create(std::move(result), input_rows_count);
        }
        else
        {
            convertLowCardinalityColumnsToFull(columns_without_low_cardinality);
            result = executeWithoutLowCardinalityColumns(columns_without_low_cardinality, result_type, input_rows_count, dry_run);
        }
    }
    else
        result = executeWithoutLowCardinalityColumns(arguments, result_type, input_rows_count, dry_run);

    return result;
}

ColumnPtr IExecutableFunction::execute(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const
{
    checkFunctionArgumentSizes(arguments, input_rows_count);

    if (useDefaultImplementationForReplicatedColumns())
    {
        /// If we have only constants and replicated columns with the same indexes
        /// we can execute function on nested columns and create replicated column
        /// from the result using common indexes.
        ColumnPtr common_replicated_indexes;
        bool has_full_columns = false;
        size_t nested_column_size = 0;
        for (const auto & argument : arguments)
        {
            if (const auto * column_replicated = typeid_cast<const ColumnReplicated *>(argument.column.get()))
            {
                if (!common_replicated_indexes)
                {
                    common_replicated_indexes = column_replicated->getIndexesColumn();
                    nested_column_size = column_replicated->getNestedColumn()->size();
                }
                else if (common_replicated_indexes != column_replicated->getIndexesColumn())
                {
                    common_replicated_indexes.reset();
                    break;
                }
            }
            else if (!isColumnConst(*argument.column))
            {
                has_full_columns = true;
                break;
            }
        }

        auto arguments_without_replicated = arguments;
        if (has_full_columns || !common_replicated_indexes)
        {
            convertReplicatedColumnsToFull(arguments_without_replicated);
            return executeWithoutReplicatedColumns(arguments_without_replicated, result_type, input_rows_count, dry_run);
        }

        for (auto & argument : arguments_without_replicated)
        {
            /// Replace replicated columns to their nested columns.
            if (const auto * column_replicated = typeid_cast<const ColumnReplicated *>(argument.column.get()))
                argument.column = column_replicated->getNestedColumn();
            /// Change size for constants.
            else if (const auto * column_const = checkAndGetColumn<ColumnConst>(argument.column.get()))
                argument.column = ColumnConst::create(column_const->getDataColumnPtr(), nested_column_size);
        }

        auto result = executeWithoutReplicatedColumns(arguments_without_replicated, result_type, nested_column_size, dry_run);

        if (isLazyReplicationUseful(result))
            return ColumnReplicated::create(result, common_replicated_indexes);

        return result->index(*common_replicated_indexes, 0);
    }

    return executeWithoutReplicatedColumns(arguments, result_type, input_rows_count, dry_run);
}

ColumnPtr IExecutableFunction::executeWithoutReplicatedColumns(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const
{
    bool use_default_implementation_for_sparse_columns = useDefaultImplementationForSparseColumns();
    /// DataTypeFunction does not support obtaining default (isDefaultAt())
    /// ColumnFunction does not support getting specific values.
    if (result_type->getTypeId() != TypeIndex::Function && use_default_implementation_for_sparse_columns)
    {
        size_t num_sparse_columns = 0;
        size_t num_full_columns = 0;
        size_t sparse_column_position = 0;

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto * column_sparse = checkAndGetColumn<ColumnSparse>(arguments[i].column.get());
            /// In rare case, when sparse column doesn't have default values,
            /// it's more convenient to convert it to full before execution of function.
            if (column_sparse && column_sparse->getNumberOfDefaultRows())
            {
                sparse_column_position = i;
                ++num_sparse_columns;
            }
            else if (!isColumnConst(*arguments[i].column))
            {
                ++num_full_columns;
            }
        }

        auto columns_without_sparse = arguments;
        if (num_sparse_columns == 1 && num_full_columns == 0)
        {
            auto & arg_with_sparse = columns_without_sparse[sparse_column_position];
            ColumnPtr sparse_offsets;
            {
                /// New scope to avoid possible mistakes on dangling reference.
                const auto & column_sparse = assert_cast<const ColumnSparse &>(*arg_with_sparse.column);
                sparse_offsets = column_sparse.getOffsetsPtr();
                arg_with_sparse.column = column_sparse.getValuesPtr();
            }

            size_t values_size = arg_with_sparse.column->size();
            for (size_t i = 0; i < columns_without_sparse.size(); ++i)
            {
                if (i == sparse_column_position)
                    continue;

                columns_without_sparse[i].column = columns_without_sparse[i].column->cloneResized(values_size);
            }

            auto res = executeWithoutSparseColumns(columns_without_sparse, result_type, values_size, dry_run);

            if (isColumnConst(*res))
                return res->cloneResized(input_rows_count);

            /// If default of sparse column is changed after execution of function, convert to full column.
            /// If there are any default in non-zero position after execution of function, convert to full column.
            /// Currently there is no easy way to rebuild sparse column with new offsets.
            if (!result_type->canBeInsideSparseColumns() || !res->isDefaultAt(0) || res->getNumberOfDefaultRows() != 1)
            {
                const auto & offsets_data = assert_cast<const ColumnVector<UInt64> &>(*sparse_offsets).getData();
                return res->createWithOffsets(offsets_data, *createColumnConst(res, 0), input_rows_count, /*shift=*/1);
            }

            return ColumnSparse::create(res, sparse_offsets, input_rows_count);
        }

        convertSparseColumnsToFull(columns_without_sparse);
        return executeWithoutSparseColumns(columns_without_sparse, result_type, input_rows_count, dry_run);
    }
    if (use_default_implementation_for_sparse_columns)
    {
        auto columns_without_sparse = arguments;
        convertSparseColumnsToFull(columns_without_sparse);
        return executeWithoutSparseColumns(columns_without_sparse, result_type, input_rows_count, dry_run);
    }
    return executeWithoutSparseColumns(arguments, result_type, input_rows_count, dry_run);
}

ColumnPtr IFunctionBase::execute(const DB::ColumnsWithTypeAndName& arguments, const DB::DataTypePtr& result_type,
        size_t input_rows_count, bool dry_run) const
{
    checkFunctionArgumentSizes(arguments, input_rows_count);
    return prepare(arguments)->execute(arguments, result_type, input_rows_count, dry_run);
}

const Array & IFunctionBase::getParameters() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "IFunctionBase doesn't support getParameters method");
}

IFunctionBase::Monotonicity IFunctionBase::getMonotonicityForRange(const IDataType & /*type*/, const Field & /*left*/, const Field & /*right*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} has no information about its monotonicity", getName());
}

FieldIntervalPtr IFunctionBase::getPreimage(const IDataType & /*type*/, const Field & /*point*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} has no information about its preimage", getName());
}

void IFunctionOverloadResolver::checkNumberOfArguments(size_t number_of_arguments) const
{
    if (isVariadic())
        return;

    size_t expected_number_of_arguments = getNumberOfArguments();

    if (number_of_arguments != expected_number_of_arguments)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be {}",
            getName(),
            number_of_arguments,
            expected_number_of_arguments);
}

DataTypePtr IFunctionOverloadResolver::getReturnType(const ColumnsWithTypeAndName & arguments) const
{
    if (useDefaultImplementationForLowCardinalityColumns())
    {
        bool has_low_cardinality = false;
        size_t num_full_low_cardinality_columns = 0;
        size_t num_full_ordinary_columns = 0;

        ColumnsWithTypeAndName args_without_low_cardinality(arguments);

        for (ColumnWithTypeAndName & arg : args_without_low_cardinality)
        {
            bool is_const = arg.column && isColumnConst(*arg.column);
            if (is_const)
                arg.column = arg.column->convertToFullColumnIfLowCardinality();

            if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(arg.type.get()))
            {
                arg.type = low_cardinality_type->getDictionaryType();
                has_low_cardinality = true;

                if (!is_const)
                    ++num_full_low_cardinality_columns;
            }
            else if (!is_const)
                ++num_full_ordinary_columns;
        }

        convertLowCardinalityColumnsToFull(args_without_low_cardinality);

        auto type_without_low_cardinality = getReturnTypeWithoutLowCardinality(args_without_low_cardinality);

        if (canBeExecutedOnLowCardinalityDictionary() && has_low_cardinality && num_full_low_cardinality_columns <= 1
            && num_full_ordinary_columns == 0 && type_without_low_cardinality->canBeInsideLowCardinality())
            return std::make_shared<DataTypeLowCardinality>(type_without_low_cardinality);
        return type_without_low_cardinality;
    }

    return getReturnTypeWithoutLowCardinality(arguments);
}

FunctionBasePtr IFunctionOverloadResolver::build(const ColumnsWithTypeAndName & arguments) const
{
    /// Use FunctionBaseDynamicAdaptor if default implementation for Dynamic is enabled and we have Dynamic type in arguments.
    if (useDefaultImplementationForDynamic())
    {
        checkNumberOfArguments(arguments.size());
        for (const auto & arg : arguments)
        {
            if (isDynamic(arg.type))
            {
                DataTypes data_types(arguments.size());
                for (size_t i = 0; i < arguments.size(); ++i)
                    data_types[i] = arguments[i].type;
                return std::make_shared<FunctionBaseDynamicAdaptor>(shared_from_this(), std::move(data_types));
            }
        }
    }

    /// Use FunctionBaseVariantAdaptor if default implementation for Variant is enabled and we have Variant type in arguments.
    if (useDefaultImplementationForVariant())
    {
        checkNumberOfArguments(arguments.size());

        for (const auto & arg : arguments)
        {
            if (isVariant(arg.type))
            {
                ColumnsWithTypeAndName args_copy = arguments;
                return std::make_shared<FunctionBaseVariantAdaptor>(shared_from_this(), std::move(args_copy));
            }
        }
    }

    auto return_type = getReturnType(arguments);
    return buildImpl(arguments, return_type);
}

void IFunctionOverloadResolver::getLambdaArgumentTypes(DataTypes & arguments [[maybe_unused]]) const
{
    checkNumberOfArguments(arguments.size());
    getLambdaArgumentTypesImpl(arguments);
}

DataTypePtr IFunctionOverloadResolver::getReturnTypeWithoutLowCardinality(const ColumnsWithTypeAndName & arguments) const
{
    checkNumberOfArguments(arguments.size());

    if (!arguments.empty() && useDefaultImplementationForNothing())
    {
        for (const auto & arg : arguments)
        {
            if (isNothing(arg.type))
                return std::make_shared<DataTypeNothing>();
        }
    }

    if (!arguments.empty() && useDefaultImplementationForNulls())
    {
        NullPresence null_presence = getNullPresense(arguments);

        if (null_presence.has_null_constant)
        {
            return makeNullable(std::make_shared<DataTypeNothing>());
        }
        if (null_presence.has_nullable)
        {
            Block nested_columns = createBlockWithNestedColumns(arguments);
            auto return_type = getReturnTypeImpl(ColumnsWithTypeAndName(nested_columns.begin(), nested_columns.end()));
            return makeNullable(return_type);
        }
    }

    return getReturnTypeImpl(arguments);
}

void IFunctionOverloadResolver::getLambdaArgumentTypesImpl(DataTypes & arguments [[maybe_unused]]) const
{
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} can't have lambda-expressions as arguments", getName());
}

FunctionBasePtr IFunctionOverloadResolver::buildImpl(const ColumnsWithTypeAndName & /* arguments */, const DataTypePtr & /* result_type */) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "buildImpl is not implemented for {}", getName());
}

DataTypePtr IFunctionOverloadResolver::getReturnTypeImpl(const DataTypes & /*arguments*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getReturnType is not implemented for {}", getName());
}

IFunctionBase::Monotonicity IFunction::getMonotonicityForRange(const IDataType & /*type*/, const Field & /*left*/, const Field & /*right*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} has no information about its monotonicity", getName());
}

FieldIntervalPtr IFunction::getPreimage(const IDataType & /*type*/, const Field & /*point*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} has no information about its preimage", getName());
}

DataTypePtr IFunction::getReturnTypeImpl(const DataTypes & /*arguments*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getReturnType is not implemented for {}", getName());
}

void IFunction::getLambdaArgumentTypes(DataTypes & /*arguments*/) const
{
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} can't have lambda-expressions as arguments", getName());
}


#if USE_EMBEDDED_COMPILER

llvm::Value * IFunctionBase::compile(llvm::IRBuilderBase & /*builder*/, const ValuesWithType & /*arguments*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
}

static std::optional<DataTypes> removeNullables(const DataTypes & types)
{
    bool has_nullable = false;
    for (const auto & type : types)
    {
        if (!typeid_cast<const DataTypeNullable *>(type.get()))
            continue;

        has_nullable = true;
        break;
    }

    if (has_nullable)
    {
        DataTypes filtered;
        filtered.reserve(types.size());

        for (const auto & sub_type : types)
            filtered.emplace_back(removeNullable(sub_type));

        return filtered;
    }

    return {};
}

bool IFunction::isCompilable(const DataTypes & arguments, const DataTypePtr & result_type) const
{
    if (useDefaultImplementationForNulls())
        if (auto denulled_arguments = removeNullables(arguments))
            return isCompilableImpl(*denulled_arguments, result_type);

    return isCompilableImpl(arguments, result_type);
}

llvm::Value * IFunction::compile(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & result_type) const
{
    DataTypes arguments_types;
    arguments_types.reserve(arguments.size());

    for (const auto & argument : arguments)
        arguments_types.push_back(argument.type);

    auto denulled_arguments_types = removeNullables(arguments_types);
    if (useDefaultImplementationForNulls() && denulled_arguments_types)
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);

        ValuesWithType unwrapped_arguments;
        unwrapped_arguments.reserve(arguments.size());

        std::vector<llvm::Value *> is_null_values;

        auto skip_arguments = getArgumentsThatDontParticipateInCompilation(arguments_types);
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & argument = arguments[i];
            llvm::Value * unwrapped_value = argument.value;

            bool skip_compile = (std::find(skip_arguments.begin(), skip_arguments.end(), i) != skip_arguments.end());
            if (argument.type->isNullable())
            {
                unwrapped_value = skip_compile ? nullptr : b.CreateExtractValue(argument.value, {0});
                is_null_values.emplace_back(skip_compile ? nullptr : b.CreateExtractValue(argument.value, {1}));
            }

            unwrapped_arguments.emplace_back(unwrapped_value, (*denulled_arguments_types)[i]);
        }

        auto unwrapped_return_type = getReturnTypeImpl(*denulled_arguments_types);
        auto * result = compileImpl(builder, unwrapped_arguments, unwrapped_return_type);
        auto * nullable_structure_type = toNativeType(b, makeNullable(unwrapped_return_type));
        auto * nullable_structure_value = llvm::Constant::getNullValue(nullable_structure_type);

        if (!unwrapped_return_type->isNullable())
        {
            auto * nullable_structure_with_result_value = b.CreateInsertValue(nullable_structure_value, result, {0});
            auto * nullable_structure_result_null = b.CreateExtractValue(nullable_structure_with_result_value, {1});

            for (auto * is_null_value : is_null_values)
            {
                if (is_null_value)
                    nullable_structure_result_null = b.CreateOr(nullable_structure_result_null, is_null_value);
            }

            return b.CreateInsertValue(nullable_structure_with_result_value, nullable_structure_result_null, {1});
        }
        else
        {
            /// In case defaultImplementationForNulls returns a nullable structure, we need to merge the null values.
            auto * result_value = b.CreateExtractValue(result, {0});
            auto * result_is_null = b.CreateExtractValue(result, {1});

            auto * nullable_structure_with_result_value = b.CreateInsertValue(nullable_structure_value, result_value, {0});
            auto * nullable_structure_result_null = b.CreateExtractValue(nullable_structure_with_result_value, {1});

            nullable_structure_result_null = b.CreateOr(nullable_structure_result_null, result_is_null);
            for (auto * is_null_value : is_null_values)
            {
                if (is_null_value)
                    nullable_structure_result_null = b.CreateOr(nullable_structure_result_null, is_null_value);
            }

            return b.CreateInsertValue(nullable_structure_with_result_value, nullable_structure_result_null, {1});
        }
    }

    return compileImpl(builder, arguments, result_type);
}

llvm::Value * IFunction::compileImpl(llvm::IRBuilderBase & /*builder*/, const ValuesWithType & /*arguments*/, const DataTypePtr & /*result_type*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
}

#endif

}
