#include <Functions/FunctionsConditional.h>
#include <Functions/FunctionsArray.h>
#include <Functions/FunctionsTransform.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnNullable.h>
#include <Interpreters/castColumn.h>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
}

void registerFunctionsConditional(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIf>();
    factory.registerFunction<FunctionMultiIf>();
    factory.registerFunction<FunctionCaseWithExpression>();

    /// These are obsolete function names.
    factory.registerFunction<FunctionCaseWithExpression>("caseWithExpr");
    factory.registerFunction<FunctionMultiIf>("caseWithoutExpr");
    factory.registerFunction<FunctionMultiIf>("caseWithoutExpression");
}


/// Implementation of FunctionMultiIf.

FunctionPtr FunctionMultiIf::create(const Context & context)
{
    return std::make_shared<FunctionMultiIf>(context);
}

String FunctionMultiIf::getName() const
{
    return name;
}


void FunctionMultiIf::executeImpl(Block & block, const ColumnNumbers & args, size_t result, size_t input_rows_count)
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

DataTypePtr FunctionMultiIf::getReturnTypeImpl(const DataTypes & args) const
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

    /// Conditions must be UInt8, Nullable(UInt8) or Null. If one of conditions is Nullable, the result is also Nullable.
    bool have_nullable_condition = false;

    for_conditions([&](const DataTypePtr & arg)
    {
        const IDataType * nested_type;
        if (arg->isNullable())
        {
            have_nullable_condition = true;

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

    DataTypePtr common_type_of_branches = getLeastSupertype(types_of_branches);

    return have_nullable_condition
        ? makeNullable(common_type_of_branches)
        : common_type_of_branches;
}


FunctionPtr FunctionCaseWithExpression::create(const Context & context_)
{
    return std::make_shared<FunctionCaseWithExpression>(context_);
}

FunctionCaseWithExpression::FunctionCaseWithExpression(const Context & context_)
    : context{context_}
{
}

String FunctionCaseWithExpression::getName() const
{
    return name;
}

DataTypePtr FunctionCaseWithExpression::getReturnTypeImpl(const DataTypes & args) const
{
    if (!args.size())
        throw Exception{"Function " + getName() + " expects at least 1 arguments",
            ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION};

    /// See the comments in executeImpl() to understand why we actually have to
    /// get the return type of a transform function.

    /// Get the return types of the arrays that we pass to the transform function.
    ColumnsWithTypeAndName src_array_types;
    ColumnsWithTypeAndName dst_array_types;

    for (size_t i = 1; i < (args.size() - 1); ++i)
    {
        if ((i % 2) != 0)
            src_array_types.push_back({nullptr, args[i], {}});
        else
            dst_array_types.push_back({nullptr, args[i], {}});
    }

    FunctionArray fun_array{context};

    DataTypePtr src_array_type = fun_array.getReturnType(src_array_types);
    DataTypePtr dst_array_type = fun_array.getReturnType(dst_array_types);

    /// Finally get the return type of the transform function.
    FunctionTransform fun_transform;
    ColumnsWithTypeAndName transform_args = {{nullptr, args.front(), {}}, {nullptr, src_array_type, {}},
                                             {nullptr, dst_array_type, {}}, {nullptr, args.back(), {}}};
    return fun_transform.getReturnType(transform_args);
}

void FunctionCaseWithExpression::executeImpl(Block & block, const ColumnNumbers & args, size_t result, size_t input_rows_count)
{
    if (!args.size())
        throw Exception{"Function " + getName() + " expects at least 1 arguments",
            ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION};

    /// In the following code, we turn the construction:
    /// CASE expr WHEN val[0] THEN branch[0] ... WHEN val[N-1] then branch[N-1] ELSE branchN
    /// into the construction transform(expr, src, dest, branchN)
    /// where:
    /// src  = [val[0], val[1], ..., val[N-1]]
    /// dest = [branch[0], ..., branch[N-1]]
    /// then we perform it.

    /// Create the arrays required by the transform function.
    ColumnNumbers src_array_args;
    ColumnsWithTypeAndName src_array_types;

    ColumnNumbers dst_array_args;
    ColumnsWithTypeAndName dst_array_types;

    for (size_t i = 1; i < (args.size() - 1); ++i)
    {
        if ((i % 2) != 0)
        {
            src_array_args.push_back(args[i]);
            src_array_types.push_back(block.getByPosition(args[i]));
        }
        else
        {
            dst_array_args.push_back(args[i]);
            dst_array_types.push_back(block.getByPosition(args[i]));
        }
    }

    FunctionArray fun_array{context};

    DataTypePtr src_array_type = fun_array.getReturnType(src_array_types);
    DataTypePtr dst_array_type = fun_array.getReturnType(dst_array_types);

    Block temp_block = block;

    size_t src_array_pos = temp_block.columns();
    temp_block.insert({nullptr, src_array_type, ""});

    size_t dst_array_pos = temp_block.columns();
    temp_block.insert({nullptr, dst_array_type, ""});

    fun_array.execute(temp_block, src_array_args, src_array_pos, input_rows_count);
    fun_array.execute(temp_block, dst_array_args, dst_array_pos, input_rows_count);

    /// Execute transform.
    FunctionTransform fun_transform;

    ColumnNumbers transform_args{args.front(), src_array_pos, dst_array_pos, args.back()};
    fun_transform.execute(temp_block, transform_args, result, input_rows_count);

    /// Put the result into the original block.
    block.getByPosition(result).column = std::move(temp_block.getByPosition(result).column);
}

}
