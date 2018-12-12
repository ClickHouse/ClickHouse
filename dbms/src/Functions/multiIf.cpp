#include <Functions/FunctionFactory.h>
#include <Functions/FunctionIfBase.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/castColumn.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function multiIf, which generalizes the function if.
///
/// Syntax: multiIf(cond_1, then_1, ..., cond_N, then_N, else)
/// where N >= 1.
///
/// For all 1 <= i <= N, "cond_i" has type UInt8.
/// Types of all the branches "then_i" and "else" are either of the following:
///    - numeric types for which there exists a common type;
///    - dates;
///    - dates with time;
///    - strings;
///    - arrays of such types.
///
/// Additionally the arguments, conditions or branches, support nullable types
/// and the NULL value, with a NULL condition treated as false.
class FunctionMultiIf final : public FunctionIfBase</*null_is_false=*/true>
{
public:
    static constexpr auto name = "multiIf";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionMultiIf>(context); }
    FunctionMultiIf(const Context & context) : context(context) {}

public:
    String getName() const override { return name; }
    bool useDefaultImplementationForNulls() const override { return false; }

    String getSignature() const override { return "f(cond1 MaybeNullable(UInt8), then1 T1, ..., else U) -> leastSupertype(T1, ..., U)"; }

    void executeImpl(Block & block, const ColumnNumbers & args, size_t result, size_t input_rows_count) override
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

                if (cond_col.column->isColumnConst())
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
                    if (cond_col.column->isColumnNullable())
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
                converted_columns_holder.emplace_back(castColumn(source_col, return_type, context));
                instruction.source = converted_columns_holder.back().get();
            }

            if (instruction.source && instruction.source->isColumnConst())
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
                    insert = static_cast<const ColumnUInt8 &>(*instruction.condition).getData()[i];
                else
                {
                    const ColumnNullable & condition_nullable = static_cast<const ColumnNullable &>(*instruction.condition);
                    const ColumnUInt8 & condition_nested = static_cast<const ColumnUInt8 &>(condition_nullable.getNestedColumn());
                    const NullMap & condition_null_map = condition_nullable.getNullMapData();

                    insert = !condition_null_map[i] && condition_nested.getData()[i];
                }

                if (insert)
                {
                    if (!instruction.source_is_constant)
                        res->insertFrom(*instruction.source, i);
                    else
                        res->insertFrom(static_cast<const ColumnConst &>(*instruction.source).getDataColumn(), 0);

                    break;
                }
            }
        }

        block.getByPosition(result).column = std::move(res);
    }

private:
    const Context & context;
};

void registerFunctionMultiIf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiIf>();

    /// These are obsolete function names.
    factory.registerFunction<FunctionMultiIf>("caseWithoutExpr");
    factory.registerFunction<FunctionMultiIf>("caseWithoutExpression");
}

}



