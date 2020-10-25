#include <Functions/FunctionFactory.h>
#include <Functions/FunctionIfBase.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/castColumn.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

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
class FunctionMultiIf final : public FunctionIfBase</*null_is_false=*/true>
{
public:
    static constexpr auto name = "multiIf";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionMultiIf>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
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

    void executeImpl(Block & block, const ColumnNumbers & args, size_t result, size_t input_rows_count) const override
    {
        /** We will gather values from columns in branches to result column,
        *  depending on values of conditions.
        */
        struct Instruction
        {
            const IColumn * condition = nullptr;
            const IColumn * source = nullptr;

            bool condition_always_true = false;
            bool condition_is_nullable = false;
            bool source_is_constant = false;
        };

        std::vector<Instruction> instructions;
        instructions.reserve(args.size() / 2 + 1);

        Columns converted_columns_holder;
        converted_columns_holder.reserve(instructions.size());

        const DataTypePtr & return_type = block.getByPosition(result).type;

        for (size_t i = 0; i < args.size(); i += 2)
        {
            Instruction instruction;
            size_t source_idx = i + 1;

            if (source_idx == args.size())
            {
                /// The last, "else" branch can be treated as a branch with always true condition "else if (true)".
                --source_idx;
                instruction.condition_always_true = true;
            }
            else
            {
                const ColumnWithTypeAndName & cond_col = block.getByPosition(args[i]);

                /// We skip branches that are always false.
                /// If we encounter a branch that is always true, we can finish.

                if (cond_col.column->onlyNull())
                    continue;

                if (isColumnConst(*cond_col.column))
                {
                    Field value = typeid_cast<const ColumnConst &>(*cond_col.column).getField();
                    if (value.isNull())
                        continue;
                    if (value.get<UInt64>() == 0)
                        continue;
                    instruction.condition_always_true = true;
                }
                else
                {
                    if (isColumnNullable(*cond_col.column))
                        instruction.condition_is_nullable = true;

                    instruction.condition = cond_col.column.get();
                }
            }

            const ColumnWithTypeAndName & source_col = block.getByPosition(args[source_idx]);
            if (source_col.type->equals(*return_type))
            {
                instruction.source = source_col.column.get();
            }
            else
            {
                /// Cast all columns to result type.
                converted_columns_holder.emplace_back(castColumn(source_col, return_type));
                instruction.source = converted_columns_holder.back().get();
            }

            if (instruction.source && isColumnConst(*instruction.source))
                instruction.source_is_constant = true;

            instructions.emplace_back(std::move(instruction));

            if (instructions.back().condition_always_true)
                break;
        }

        size_t rows = input_rows_count;
        MutableColumnPtr res = return_type->createColumn();

        for (size_t i = 0; i < rows; ++i)
        {
            for (const auto & instruction : instructions)
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

        block.getByPosition(result).column = std::move(res);
    }
};

void registerFunctionMultiIf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiIf>();

    /// These are obsolete function names.
    factory.registerFunction<FunctionMultiIf>("caseWithoutExpr");
    factory.registerFunction<FunctionMultiIf>("caseWithoutExpression");
}

}


