#include <Functions/FunctionsConditional.h>
#include <Functions/FunctionsArray.h>
#include <Functions/FunctionsTransform.h>
#include <Functions/FunctionFactory.h>
#include <Functions/Conditional/common.h>
#include <Functions/Conditional/NullMapBuilder.h>
#include <Functions/Conditional/CondSource.h>
#include <Functions/Conditional/NumericPerformer.h>
#include <Functions/Conditional/StringEvaluator.h>
#include <Functions/Conditional/StringArrayEvaluator.h>
#include <Columns/ColumnNullable.h>

namespace DB
{

void registerFunctionsConditional(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIf>();
    factory.registerFunction<FunctionMultiIf>();
    factory.registerFunction<FunctionCaseWithExpression>();
    factory.registerFunction<FunctionCaseWithoutExpression>();

    /// These are obsolete function names.
    factory.registerFunction("caseWithExpr", FunctionCaseWithExpression::create);
    factory.registerFunction("caseWithoutExpr", FunctionCaseWithoutExpression::create);
}


namespace
{

/// Check whether at least one of the specified branches of the function multiIf
/// is either a nullable column or a null column inside a given block.
bool blockHasSpecialBranches(const Block & block, const ColumnNumbers & args)
{
    auto check = [](const Block & block, size_t arg)
    {
        const auto & elem = block.getByPosition(arg);
        return elem.column->isNullable() || elem.column->isNull();
    };

    size_t else_arg = Conditional::elseArg(args);
    for (size_t i = Conditional::firstThen(); i < else_arg; i = Conditional::nextThen(i))
    {
        if (check(block, args[i]))
            return true;
    }

    if (check(block, args[else_arg]))
        return true;

    return false;
}

/// Check whether at least one of the specified datatypes is either nullable or null.
bool hasSpecialDataTypes(const DataTypes & args)
{
    size_t else_arg = Conditional::elseArg(args);

    for (size_t i = Conditional::firstThen(); i < else_arg; i = Conditional::nextThen(i))
    {
        if (args[i]->isNullable() || args[i]->isNull())
            return true;
    }

    return args[else_arg]->isNullable() || args[else_arg]->isNull();
}

/// Return the type of the first non-null branch. Make it nullable
/// if there is at least one nullable branch or one null branch.
/// This function is used in a very few number of cases in getReturnTypeImpl().
DataTypePtr getReturnTypeFromFirstNonNullBranch(const DataTypes & args, bool has_special_types)
{
    auto get_type_to_return = [has_special_types](const DataTypePtr & arg) -> DataTypePtr
    {
        if (arg->isNullable())
            return arg;
        else if (has_special_types)
            return std::make_shared<DataTypeNullable>(arg);
        else
            return arg;
    };

    for (size_t i = Conditional::firstThen(); i < Conditional::elseArg(args); i = Conditional::nextThen(i))
    {
        if (!args[i]->isNull())
            return get_type_to_return(args[i]);
    }

    size_t i = Conditional::elseArg(args);
    if (!args[i]->isNull())
        return get_type_to_return(args[i]);

    return {};
}

}

/// Implementation of FunctionMultiIf.

FunctionPtr FunctionMultiIf::create(const Context &)
{
    return std::make_shared<FunctionMultiIf>();
}

String FunctionMultiIf::getName() const
{
    return name;
}

void FunctionMultiIf::executeImpl(Block & block, const ColumnNumbers & args, size_t result)
{
    if (!blockHasSpecialBranches(block, args))
    {
        /// All the branch types are ordinary. No special processing required.
        Conditional::NullMapBuilder builder;
        perform(block, args, result, builder);
        return;
    }

    /// From the block to be processed, deduce a block whose specified
    /// columns are not nullable. We accept null columns because they
    /// are processed independently later.
    ColumnNumbers args_to_transform;
    size_t else_arg = Conditional::elseArg(args);
    for (size_t i = Conditional::firstThen(); i < else_arg; i = Conditional::nextThen(i))
        args_to_transform.push_back(args[i]);
    args_to_transform.push_back(args[else_arg]);

    Block block_with_nested_cols = createBlockWithNestedColumns(block, args_to_transform);

    /// Create an object that will incrementally build the null map of the
    /// result column to be returned.
    Conditional::NullMapBuilder builder{block};

    /// Now perform multiIf.
    perform(block_with_nested_cols, args, result, builder);

    /// Store the result.
    const ColumnWithTypeAndName & source_col = block_with_nested_cols.getByPosition(result);
    ColumnWithTypeAndName & dest_col = block.getByPosition(result);

    if (source_col.column->isNull())
        dest_col.column = source_col.column;
    else
        dest_col.column = std::make_shared<ColumnNullable>(source_col.column, builder.getNullMap());
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

    if (!Conditional::hasValidArgCount(args))
        throw Exception{"Invalid number of arguments for function " + getName(),
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    /// Conditions must be UInt8, Nullable(UInt8) or Null. If one of conditions is Nullable, the result is also Nullable.
    bool have_nullable_condition = false;
    bool all_conditions_are_null = true;

    for_conditions([&](const DataTypePtr & arg)
    {
        const IDataType * observed_type;
        if (arg->isNullable())
        {
            have_nullable_condition = true;
            all_conditions_are_null = false;
            const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(*arg);
            observed_type = nullable_type.getNestedType().get();
        }
        else if (arg->isNull())
        {
            have_nullable_condition = true;
        }
        else
        {
            all_conditions_are_null = false;
            observed_type = arg.get();
        }

        if (!checkDataType<DataTypeUInt8>(observed_type) && !observed_type->isNull())
            throw Exception{"Illegal type of argument " + toString(i) + " (condition) "
                "of function " + getName() + ". Must be UInt8.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    });

    if (all_conditions_are_null)
        return std::make_shared<DataTypeNull>();

    DataTypes types_of_branches;
    types_of_branches.reserve(Conditional::getBranchCount(args));

    for_branches([&](const DataTypePtr & arg)
    {
        types_of_branches.emplace_back(arg);
    });

    DataTypePtr common_type_of_branches = getLeastCommonType(types_of_branches);

    return have_nullable_condition
        ? makeNullableDataTypeIfNot(common_type_of_branches)
        : common_type_of_branches;
}

void FunctionMultiIf::perform(Block & block, const ColumnNumbers & args, size_t result, Conditional::NullMapBuilder & builder)
{
    if (performTrivialCase(block, args, result, builder))
        return;
    if (Conditional::NumericPerformer::perform(block, args, result, builder))
        return;
    if (Conditional::StringEvaluator::perform(block, args, result, builder))
        return;
    if (Conditional::StringArrayEvaluator::perform(block, args, result, builder))
        return;

    throw Exception{"One or more branch (then, else) columns of function "
        + getName() + " have illegal or incompatible types",
        ErrorCodes::ILLEGAL_COLUMN};
}

bool FunctionMultiIf::performTrivialCase(Block & block, const ColumnNumbers & args,
    size_t result, Conditional::NullMapBuilder & builder)
{
    /// Check that all the branches have the same type. Moreover
    /// some or all these branches may be null.
    DataTypePtr type;

    size_t else_arg = Conditional::elseArg(args);
    for (size_t i = Conditional::firstThen(); i < else_arg; i = Conditional::nextThen(i))
    {
        if (!block.getByPosition(args[i]).type->isNull())
        {
            if (!type)
                type = block.getByPosition(args[i]).type;
            else if (!type->equals(*block.getByPosition(args[i]).type))
                return false;
        }
    }

    if (!block.getByPosition(args[else_arg]).type->isNull())
    {
        if (!type)
            type = block.getByPosition(args[else_arg]).type;
        else if (!type->equals(*block.getByPosition(args[else_arg]).type))
            return false;
    }

    size_t row_count = block.rows();
    auto & res_col = block.getByPosition(result).column;

    if (!type)
    {
        /// Degenerate case: all the branches are null.
        res_col = block.getByPosition(result).type->createConstColumn(row_count, Null());
        return true;
    }

    /// Check that all the conditions are constants.
    for (size_t i = Conditional::firstCond(); i < else_arg; i = Conditional::nextCond(i))
    {
        const IColumn * col = block.getByPosition(args[i]).column.get();
        if (!col->isConst())
            return false;
    }

    /// Initialize readers for the conditions.
    Conditional::CondSources conds;
    conds.reserve(Conditional::getCondCount(args));

    for (size_t i = Conditional::firstCond(); i < else_arg; i = Conditional::nextCond(i))
        conds.emplace_back(block, args, i);

    /// Perform multiIf.

    auto make_result = [&](size_t index)
    {
        res_col = block.getByPosition(index).column;
        if (res_col->isNull())
        {
            /// The return type of multiIf is Nullable(T). Therefore we create
            /// a constant column whose type is T with a default value.
            /// Subsequently the null map builder will mark it as null.
            res_col = type->createConstColumn(row_count, type->getDefault());
        }
        if (builder)
            builder.build(index);
    };

    size_t i = Conditional::firstCond();
    for (const auto & cond : conds)
    {
        if (cond.get(0))
        {
            make_result(args[Conditional::thenFromCond(i)]);
            return true;
        }
        i = Conditional::nextCond(i);
    }

    make_result(args[else_arg]);
    return true;
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
    /// See the comments in executeImpl() to understand why we actually have to
    /// get the return type of a transform function.

    /// Get the return types of the arrays that we pass to the transform function.
    DataTypes src_array_types;
    DataTypes dst_array_types;

    for (size_t i = 1; i < (args.size() - 1); ++i)
    {
        if ((i % 2) != 0)
            src_array_types.push_back(args[i]);
        else
            dst_array_types.push_back(args[i]);
    }

    FunctionArray fun_array{context};

    DataTypePtr src_array_type = fun_array.getReturnTypeImpl(src_array_types);
    DataTypePtr dst_array_type = fun_array.getReturnTypeImpl(dst_array_types);

    /// Finally get the return type of the transform function.
    FunctionTransform fun_transform;
    return fun_transform.getReturnTypeImpl({args.front(), src_array_type, dst_array_type, args.back()});
}

void FunctionCaseWithExpression::executeImpl(Block & block, const ColumnNumbers & args, size_t result)
{
    /// In the following code, we turn the construction:
    /// CASE expr WHEN val[0] THEN branch[0] ... WHEN val[N-1] then branch[N-1] ELSE branchN
    /// into the construction transform(expr, src, dest, branchN)
    /// where:
    /// src  = [val[0], val[1], ..., val[N-1]]
    /// dest = [branch[0], ..., branch[N-1]]
    /// then we perform it.

    /// Create the arrays required by the transform function.
    ColumnNumbers src_array_args;
    DataTypes src_array_types;

    ColumnNumbers dst_array_args;
    DataTypes dst_array_types;

    for (size_t i = 1; i < (args.size() - 1); ++i)
    {
        if ((i % 2) != 0)
        {
            src_array_args.push_back(args[i]);
            src_array_types.push_back(block.getByPosition(args[i]).type);
        }
        else
        {
            dst_array_args.push_back(args[i]);
            dst_array_types.push_back(block.getByPosition(args[i]).type);
        }
    }

    FunctionArray fun_array{context};

    DataTypePtr src_array_type = fun_array.getReturnTypeImpl(src_array_types);
    DataTypePtr dst_array_type = fun_array.getReturnTypeImpl(dst_array_types);

    Block temp_block = block;

    size_t src_array_pos = temp_block.columns();
    temp_block.insert({nullptr, src_array_type, ""});

    size_t dst_array_pos = temp_block.columns();
    temp_block.insert({nullptr, dst_array_type, ""});

    fun_array.executeImpl(temp_block, src_array_args, src_array_pos);
    fun_array.executeImpl(temp_block, dst_array_args, dst_array_pos);

    /// Execute transform.
    FunctionTransform fun_transform;

    ColumnNumbers transform_args{args.front(), src_array_pos, dst_array_pos, args.back()};
    fun_transform.executeImpl(temp_block, transform_args, result);

    /// Put the result into the original block.
    block.getByPosition(result).column = std::move(temp_block.getByPosition(result).column);
}


FunctionPtr FunctionCaseWithoutExpression::create(const Context &)
{
    return std::make_shared<FunctionCaseWithoutExpression>();
}

String FunctionCaseWithoutExpression::getName() const
{
    return name;
}

DataTypePtr FunctionCaseWithoutExpression::getReturnTypeImpl(const DataTypes & args) const
{
    FunctionMultiIf fun_multi_if;
    return fun_multi_if.getReturnTypeImpl(args);
}

void FunctionCaseWithoutExpression::executeImpl(Block & block, const ColumnNumbers & args, size_t result)
{
    /// A CASE construction without any expression is a straightforward multiIf.
    FunctionMultiIf fun_multi_if;
    fun_multi_if.executeImpl(block, args, result);
}

}
