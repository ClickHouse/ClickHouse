#include <Functions/FunctionFactory.h>
#include <Functions/FunctionIfBase.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/MaskOperations.h>
#include <Interpreters/castColumn.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMultiIf>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    bool isShortCircuit(ShortCircuitSettings & settings, size_t number_of_arguments) const override
    {
        settings.enable_lazy_execution_for_first_argument = false;
        settings.enable_lazy_execution_for_common_descendants_of_arguments = (number_of_arguments != 3);
        settings.force_enable_lazy_execution = false;
        return true;
    }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForNulls() const override { return false; }

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
            throw Exception{"Invalid number of arguments for function " + getName(),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

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
                throw Exception{"Illegal type " + arg->getName() + " of argument (condition) "
                    "of function " + getName() + ". Must be UInt8.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        });

        DataTypes types_of_branches;
        types_of_branches.reserve(args.size() / 2 + 1);

        for_branches([&](const DataTypePtr & arg)
        {
            types_of_branches.emplace_back(arg);
        });

        return getLeastSupertype(types_of_branches);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnsWithTypeAndName arguments = args;
        executeShortCircuitArguments(arguments);
        /** We will gather values from columns in branches to result column,
        *  depending on values of conditions.
        */
        struct Instruction
        {
            IColumn::Ptr condition = nullptr;
            IColumn::Ptr source = nullptr;

            bool condition_always_true = false;
            bool condition_is_nullable = false;
            bool source_is_constant = false;

            bool condition_is_short = false;
            bool source_is_short = false;
            size_t condition_index = 0;
            size_t source_index = 0;
        };

        std::vector<Instruction> instructions;
        instructions.reserve(arguments.size() / 2 + 1);

        Columns converted_columns_holder;
        converted_columns_holder.reserve(instructions.size());

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

                if (const auto * column_const = checkAndGetColumn<ColumnConst>(*cond_col))
                {
                    Field value = column_const->getField();

                    if (value.isNull())
                        continue;
                    if (value.get<UInt64>() == 0)
                        continue;

                    instruction.condition_always_true = true;
                }
                else
                {
                    instruction.condition = cond_col;
                    instruction.condition_is_nullable = instruction.condition->isNullable();
                }

                instruction.condition_is_short = cond_col->size() < arguments[0].column->size();
            }

            const ColumnWithTypeAndName & source_col = arguments[source_idx];
            instruction.source_is_short = source_col.column->size() < arguments[0].column->size();
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

        MutableColumnPtr res = return_type->createColumn();

        /// Special case if first instruction condition is always true and source is constant
        if (instructions.size() == 1 && instructions.front().source_is_constant
            && instructions.front().condition_always_true)
        {
            auto & instruction = instructions.front();
            res->insertFrom(assert_cast<const ColumnConst &>(*instruction.source).getDataColumn(), 0);
            return ColumnConst::create(std::move(res), instruction.source->size());
        }

        size_t rows = input_rows_count;

        for (size_t i = 0; i < rows; ++i)
        {
            for (auto & instruction : instructions)
            {
                bool insert = false;

                size_t condition_index = instruction.condition_is_short ? instruction.condition_index++ : i;
                if (instruction.condition_always_true)
                    insert = true;
                else if (!instruction.condition_is_nullable)
                    insert = assert_cast<const ColumnUInt8 &>(*instruction.condition).getData()[condition_index];
                else
                {
                    const ColumnNullable & condition_nullable = assert_cast<const ColumnNullable &>(*instruction.condition);
                    const ColumnUInt8 & condition_nested = assert_cast<const ColumnUInt8 &>(condition_nullable.getNestedColumn());
                    const NullMap & condition_null_map = condition_nullable.getNullMapData();

                    insert = !condition_null_map[condition_index] && condition_nested.getData()[condition_index];
                }

                if (insert)
                {
                    size_t source_index = instruction.source_is_short ? instruction.source_index++ : i;
                    if (!instruction.source_is_constant)
                        res->insertFrom(*instruction.source, source_index);
                    else
                        res->insertFrom(assert_cast<const ColumnConst &>(*instruction.source).getDataColumn(), 0);

                    break;
                }
            }
        }

        return res;
    }

private:
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
};

}

REGISTER_FUNCTION(MultiIf)
{
    factory.registerFunction<FunctionMultiIf>();

    /// These are obsolete function names.
    factory.registerFunction<FunctionMultiIf>("caseWithoutExpr");
    factory.registerFunction<FunctionMultiIf>("caseWithoutExpression");
}

}


