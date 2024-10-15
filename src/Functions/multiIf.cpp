#include <Functions/FunctionFactory.h>
#include <Functions/FunctionIfBase.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/MaskOperations.h>
#include <Core/Settings.h>
#include <Interpreters/castColumn.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/getLeastSupertype.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_execute_multiif_columnar;
    extern const SettingsBool allow_experimental_variant_type;
    extern const SettingsBool use_variant_as_common_type;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Function multiIf, which generalizes the function if.
///
/// Syntax: multiIf(cond_1, then_1, ..., cond_N, then_N, else)
/// where N >= 1.
///
/// For all 1 <= i <= N, "cond_i" has type UInt8.
/// Types of all the branches "then_i" and "else" have a common type.
///
/// Additionally the arguments, conditions or branches, support nullable types
/// and the NULL value, with a NULL condition treated as false.
class FunctionMultiIf final : public FunctionIfBase
{
public:
    static constexpr auto name = "multiIf";
    static FunctionPtr create(ContextPtr context_)
    {
        const auto & settings = context_->getSettingsRef();
        return std::make_shared<FunctionMultiIf>(
            settings[Setting::allow_execute_multiif_columnar], settings[Setting::allow_experimental_variant_type], settings[Setting::use_variant_as_common_type]);
    }

    explicit FunctionMultiIf(bool allow_execute_multiif_columnar_, bool allow_experimental_variant_type_, bool use_variant_as_common_type_)
        : allow_execute_multiif_columnar(allow_execute_multiif_columnar_)
        , allow_experimental_variant_type(allow_experimental_variant_type_)
        , use_variant_as_common_type(use_variant_as_common_type_)
    {}

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    bool isShortCircuit(ShortCircuitSettings & settings, size_t number_of_arguments) const override
    {
        settings.arguments_with_disabled_lazy_execution.insert(0);
        settings.enable_lazy_execution_for_common_descendants_of_arguments = (number_of_arguments != 3);
        settings.force_enable_lazy_execution = false;
        return true;
    }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool canBeExecutedOnLowCardinalityDictionary() const override { return false; }

    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t number_of_arguments) const override
    {
        ColumnNumbers args;
        for (size_t i = 0; i + 1 < number_of_arguments; i += 2)
            args.push_back(i);
        return args;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & args) const override
    {
        /// Arguments are the following: cond1, then1, cond2, then2, ... condN, thenN, else.

        auto for_conditions = [&args](auto && f)
        {
            size_t conditions_end = args.size() - 1;
            for (size_t i = 0; i < conditions_end; i += 2)
                f(args[i]);
        };

        auto for_branches = [&args](auto && f)
        {
            size_t branches_end = args.size();
            for (size_t i = 1; i < branches_end; i += 2)
                f(args[i]);
            f(args.back());
        };

        if (!(args.size() >= 3 && args.size() % 2 == 1))
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Invalid number of arguments for function {}", getName());

        for_conditions([&](const DataTypePtr & arg)
        {
            const IDataType * nested_type;
            if (arg->isNullable())
            {
                if (arg->onlyNull())
                    return;

                const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(*arg);
                nested_type = nullable_type.getNestedType().get();
            }
            else
            {
                nested_type = arg.get();
            }

            if (!WhichDataType(nested_type).isUInt8())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument (condition) of function {}. "
                    "Must be UInt8.", arg->getName(), getName());
        });

        DataTypes types_of_branches;
        types_of_branches.reserve(args.size() / 2 + 1);

        for_branches([&](const DataTypePtr & arg)
        {
            types_of_branches.emplace_back(arg);
        });

        if (allow_experimental_variant_type && use_variant_as_common_type)
            return getLeastSupertypeOrVariant(types_of_branches);

        return getLeastSupertype(types_of_branches);
    }

    struct Instruction
    {
        IColumn::Ptr condition = nullptr;
        IColumn::Ptr source = nullptr;

        bool condition_always_true = false;
        bool condition_is_nullable = false;
        bool source_is_constant = false;
    };

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Fast path when data is empty
        if (input_rows_count == 0)
            return result_type->createColumn();

        ColumnsWithTypeAndName arguments = args;
        executeShortCircuitArguments(arguments);
        /** We will gather values from columns in branches to result column,
        *  depending on values of conditions.
        */

        std::vector<Instruction> instructions;
        instructions.reserve(arguments.size() / 2 + 1);

        Columns converted_columns_holder;
        converted_columns_holder.reserve(instructions.capacity());

        const DataTypePtr & return_type = result_type;

        for (size_t i = 0; i < arguments.size(); i += 2)
        {
            Instruction instruction;
            size_t source_idx = i + 1;

            bool last_else_branch = source_idx == arguments.size();

            if (last_else_branch)
            {
                /// The last, "else" branch can be treated as a branch with always true condition "else if (true)".
                --source_idx;
                instruction.condition_always_true = true;
            }
            else
            {
                IColumn::Ptr cond_col = arguments[i].column->convertToFullColumnIfLowCardinality();

                /// We skip branches that are always false.
                /// If we encounter a branch that is always true, we can finish.

                if (cond_col->onlyNull())
                    continue;

                if (const auto * column_const = checkAndGetColumn<ColumnConst>(&*cond_col))
                {
                    Field value = column_const->getField();

                    if (value.isNull())
                        continue;
                    if (value.safeGet<UInt64>() == 0)
                        continue;

                    instruction.condition_always_true = true;
                }
                else
                {
                    instruction.condition = cond_col;
                    instruction.condition_is_nullable = instruction.condition->isNullable();
                }
            }

            const ColumnWithTypeAndName & source_col = arguments[source_idx];
            if (source_col.type->equals(*return_type))
            {
                instruction.source = source_col.column;
            }
            else
            {
                /// Cast all columns to result type.
                converted_columns_holder.emplace_back(castColumn(source_col, return_type));
                instruction.source = converted_columns_holder.back();
            }

            if (instruction.source && isColumnConst(*instruction.source))
                instruction.source_is_constant = true;

            instructions.emplace_back(std::move(instruction));

            if (instructions.back().condition_always_true)
                break;
        }

        /// Special case if first instruction condition is always true and source is constant
        if (instructions.size() == 1 && instructions.front().source_is_constant
            && instructions.front().condition_always_true)
        {
            MutableColumnPtr res = return_type->createColumn();
            auto & instruction = instructions.front();
            res->insertFrom(assert_cast<const ColumnConst &>(*instruction.source).getDataColumn(), 0);
            return ColumnConst::create(std::move(res), instruction.source->size());
        }

        const WhichDataType which(removeNullable(result_type));
        bool execute_multiif_columnar = allow_execute_multiif_columnar && instructions.size() <= std::numeric_limits<UInt8>::max()
            && (which.isInt() || which.isUInt() || which.isFloat() || which.isDecimal() || which.isDateOrDate32OrDateTimeOrDateTime64()
                || which.isEnum() || which.isIPv4() || which.isIPv6());

        size_t rows = input_rows_count;
        if (!execute_multiif_columnar)
        {
            MutableColumnPtr res = return_type->createColumn();
            res->reserve(rows);
            executeInstructions(instructions, rows, res);
            return std::move(res);
        }

#define EXECUTE_INSTRUCTIONS_COLUMNAR(TYPE, FIELD, INDEX) \
    if (which.is##TYPE()) \
    { \
        MutableColumnPtr res = result_type->createColumn(); \
        if (result_type->isNullable()) \
        { \
            auto & res_nullable = assert_cast<ColumnNullable &>(*res); \
            auto & res_data = assert_cast<ColumnVectorOrDecimal<FIELD> &>(res_nullable.getNestedColumn()).getData(); \
            auto & res_null_map = res_nullable.getNullMapData(); \
            executeInstructionsColumnar<FIELD, INDEX, true>(instructions, rows, res_data, &res_null_map); \
        } \
        else \
        { \
            auto & res_data = assert_cast<ColumnVectorOrDecimal<FIELD> &>(*res).getData(); \
            executeInstructionsColumnar<FIELD, INDEX, false>(instructions, rows, res_data, nullptr); \
        } \
        return std::move(res); \
    }

#define ENUMERATE_NUMERIC_TYPES(M, INDEX) \
    M(UInt8, UInt8, INDEX) \
    M(UInt16, UInt16, INDEX) \
    M(UInt32, UInt32, INDEX) \
    M(UInt64, UInt64, INDEX) \
    M(Int8, Int8, INDEX) \
    M(Int16, Int16, INDEX) \
    M(Int32, Int32, INDEX) \
    M(Int64, Int64, INDEX) \
    M(Float32, Float32, INDEX) \
    M(Float64, Float64, INDEX) \
    M(UInt128, UInt128, INDEX) \
    M(UInt256, UInt256, INDEX) \
    M(Int128, Int128, INDEX) \
    M(Int256, Int256, INDEX) \
    M(Decimal32, Decimal32, INDEX) \
    M(Decimal64, Decimal64, INDEX) \
    M(Decimal128, Decimal128, INDEX) \
    M(Decimal256, Decimal256, INDEX) \
    M(Date, UInt16, INDEX) \
    M(Date32, Int32, INDEX) \
    M(DateTime, UInt32, INDEX) \
    M(DateTime64, DateTime64, INDEX) \
    M(Enum8, Int8, INDEX) \
    M(Enum16, Int16, INDEX) \
    M(IPv4, IPv4, INDEX) \
    M(IPv6, IPv6, INDEX) \
    throw Exception( \
        ErrorCodes::NOT_IMPLEMENTED, "Columnar execution of function {} not implemented for type {}", getName(), result_type->getName());

        ENUMERATE_NUMERIC_TYPES(EXECUTE_INSTRUCTIONS_COLUMNAR, UInt8)
    }
#undef ENUMERATE_NUMERIC_TYPES
#undef EXECUTE_INSTRUCTIONS_COLUMNAR

private:

    static void executeInstructions(std::vector<Instruction> & instructions, size_t rows, const MutableColumnPtr & res)
    {
        for (size_t i = 0; i < rows; ++i)
        {
            for (auto & instruction : instructions)
            {
                bool insert = false;

                if (instruction.condition_always_true)
                    insert = true;
                else if (!instruction.condition_is_nullable)
                    insert = assert_cast<const ColumnUInt8 &>(*instruction.condition).getData()[i];
                else
                {
                    const ColumnNullable & condition_nullable = assert_cast<const ColumnNullable &>(*instruction.condition);
                    const ColumnUInt8 & condition_nested = assert_cast<const ColumnUInt8 &>(condition_nullable.getNestedColumn());
                    const NullMap & condition_null_map = condition_nullable.getNullMapData();

                    insert = !condition_null_map[i] && condition_nested.getData()[i];
                }

                if (insert)
                {
                    if (!instruction.source_is_constant)
                        res->insertFrom(*instruction.source, i);
                    else
                        res->insertFrom(assert_cast<const ColumnConst &>(*instruction.source).getDataColumn(), 0);

                    break;
                }
            }
        }
    }

    /// We should read source from which instruction on each row?
    template <typename S>
    static NO_INLINE void calculateInserts(const std::vector<Instruction> & instructions, size_t rows, PaddedPODArray<S> & inserts)
    {
        for (S i = instructions.size() - 1; i != static_cast<S>(-1); --i)
        {
            const auto & instruction = instructions[i];
            if (instruction.condition_always_true)
            {
                for (size_t row_i = 0; row_i < rows; ++row_i)
                    inserts[row_i] = i;
            }
            else if (!instruction.condition_is_nullable)
            {
                const auto & cond_data = assert_cast<const ColumnUInt8 &>(*instruction.condition).getData();
                for (size_t row_i = 0; row_i < rows; ++row_i)
                {
                    /// Equivalent to below code. But it is able to utilize SIMD instructions.
                    /// if (cond_data[row_i])
                    ///     inserts[row_i] = i;

                    inserts[row_i] += (!!cond_data[row_i]) * (i - inserts[row_i]);
                }
            }
            else
            {
                const ColumnNullable & condition_nullable = assert_cast<const ColumnNullable &>(*instruction.condition);
                const ColumnUInt8 & condition_nested = assert_cast<const ColumnUInt8 &>(condition_nullable.getNestedColumn());
                const auto & condition_nested_data = condition_nested.getData();
                const NullMap & condition_null_map = condition_nullable.getNullMapData();

                for (size_t row_i = 0; row_i < rows; ++row_i)
                {
                    /// Equivalent to below code. But it is able to utilize SIMD instructions.
                    /// if (!condition_null_map[row_i] && condition_nested_data[row_i])
                    ///     inserts[row_i] = i;
                    inserts[row_i] += (~condition_null_map[row_i] & (!!condition_nested_data[row_i])) * (i - inserts[row_i]);
                }
            }
        }
    }

    template <typename T, typename S, bool nullable_result = false>
    static NO_INLINE void executeInstructionsColumnar(
        const std::vector<Instruction> & instructions,
        size_t rows,
        PaddedPODArray<T> & res_data,
        PaddedPODArray<UInt8> * res_null_map = nullptr)
    {
        PaddedPODArray<S> inserts(rows, static_cast<S>(instructions.size()));
        calculateInserts(instructions, rows, inserts);

        res_data.resize_exact(rows);
        if constexpr (nullable_result)
        {
            if (!res_null_map)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid result null_map while result type is nullable");

            res_null_map->resize_exact(rows);
        }

        std::vector<const T *> data_cols(instructions.size(), nullptr);
        std::vector<const UInt8 *> null_map_cols(instructions.size(), nullptr);
        for (size_t i = 0; i < instructions.size(); ++i)
        {
            const auto & instruction = instructions[i];
            const IColumn * non_const_col = instructions[i].source_is_constant
                ? &assert_cast<const ColumnConst &>(*instruction.source).getDataColumn()
                : instruction.source.get();
            const ColumnNullable * nullable_col = checkAndGetColumn<ColumnNullable>(non_const_col);
            data_cols[i] = nullable_col ? assert_cast<const ColumnVectorOrDecimal<T> &>(nullable_col->getNestedColumn()).getData().data()
                                        : assert_cast<const ColumnVectorOrDecimal<T> &>(*non_const_col).getData().data();
            null_map_cols[i] = nullable_col ? assert_cast<const ColumnUInt8 &>(nullable_col->getNullMapColumn()).getData().data() : nullptr;
        }

        std::unique_ptr<PaddedPODArray<UInt8>> shared_null_map;
        if constexpr (nullable_result)
        {
            for (auto & col : null_map_cols)
            {
                if (!col)
                {
                    if (!shared_null_map)
                        shared_null_map = std::make_unique<PaddedPODArray<UInt8>>(rows, 0);

                    col = shared_null_map->data();
                }
            }
        }

        for (size_t row_i = 0; row_i < rows; ++row_i)
        {
            S insert = inserts[row_i];
            const auto & instruction = instructions[insert];
            size_t index = instruction.source_is_constant ? 0 : row_i;
            res_data[row_i] = *(data_cols[insert] + index);
            if constexpr (nullable_result)
                (*res_null_map)[row_i] = *(null_map_cols[insert] + index);
        }
    }

    static void executeShortCircuitArguments(ColumnsWithTypeAndName & arguments)
    {
        int last_short_circuit_argument_index = checkShortCircuitArguments(arguments);
        if (last_short_circuit_argument_index < 0)
            return;

        executeColumnIfNeeded(arguments[0]);

        /// Let's denote x_i' = maskedExecute(x_i, mask).
        /// multiIf(x_0, y_0, x_1, y_1, x_2, y_2, ..., x_{n-1}, y_{n-1}, y_n)
        /// We will support mask_i = !x_0 & !x_1 & ... & !x_i
        /// and condition_i = !x_0 & ... & !x_{i - 1} & x_i
        /// Base:
        /// mask_0 and condition_0 is 1 everywhere, x_0' = x_0.
        /// Iteration:
        /// condition_i = extractMask(mask_{i - 1}, x_{i - 1}')
        /// y_i' = maskedExecute(y_i, condition)
        /// mask_i = extractMask(mask_{i - 1}, !x_{i - 1}')
        /// x_i' = maskedExecute(x_i, mask)
        /// Also we will treat NULL as 0 if x_i' is Nullable.

        IColumn::Filter mask(arguments[0].column->size(), 1);
        MaskInfo mask_info = {.has_ones = true, .has_zeros = false};
        IColumn::Filter condition_mask(arguments[0].column->size());
        MaskInfo condition_mask_info = {.has_ones = true, .has_zeros = false};

        int i = 1;
        while (i <= last_short_circuit_argument_index)
        {
            auto & cond_column = arguments[i - 1].column;
            /// If condition is const or null and value is false, we can skip execution of expression after this condition.
            if ((isColumnConst(*cond_column) || cond_column->onlyNull()) && !cond_column->empty() && !cond_column->getBool(0))
            {
                condition_mask_info.has_ones = false;
                condition_mask_info.has_zeros = true;
            }
            else
            {
                copyMask(mask, condition_mask);
                condition_mask_info = extractMask(condition_mask, cond_column);
                maskedExecute(arguments[i], condition_mask, condition_mask_info);
            }

            /// Check if the condition is always true and we don't need to execute the rest arguments.
            if (!condition_mask_info.has_zeros)
                break;

            ++i;
            if (i > last_short_circuit_argument_index)
                break;

            /// Extract mask only if it make sense.
            if (condition_mask_info.has_ones)
                mask_info = extractInvertedMask(mask, cond_column);

            /// mask is a inverted disjunction of previous conditions and if it doesn't have once, we don't need to execute the rest arguments.
            if (!mask_info.has_ones)
                break;

            maskedExecute(arguments[i], mask, mask_info);
            ++i;
        }

        /// We could skip some arguments execution, but we cannot leave them as ColumnFunction.
        /// So, create an empty column with the execution result type.
        for (; i <= last_short_circuit_argument_index; ++i)
            executeColumnIfNeeded(arguments[i], true);
    }

    const bool allow_execute_multiif_columnar;
    const bool allow_experimental_variant_type;
    const bool use_variant_as_common_type;
};

}

REGISTER_FUNCTION(MultiIf)
{
    factory.registerFunction<FunctionMultiIf>();

    /// These are obsolete function names.
    factory.registerAlias("caseWithoutExpr", "multiIf");
    factory.registerAlias("caseWithoutExpression", "multiIf");
}

FunctionOverloadResolverPtr createInternalMultiIfOverloadResolver(bool allow_execute_multiif_columnar, bool allow_experimental_variant_type, bool use_variant_as_common_type)
{
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionMultiIf>(allow_execute_multiif_columnar, allow_experimental_variant_type, use_variant_as_common_type));
}

}
