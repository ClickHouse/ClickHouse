#include "AggregateFunctionCombinatorFactory.h"
#include "AggregateFunctionIf.h"
#include "AggregateFunctionNull.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class AggregateFunctionCombinatorIf final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "If"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments for aggregate function with {} suffix", getName());

        if (!isUInt8(arguments.back()) && !arguments.back()->onlyNull())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of last argument for "
                            "aggregate function with {} suffix", arguments.back()->getName(), getName());

        return DataTypes(arguments.begin(), std::prev(arguments.end()));
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        return std::make_shared<AggregateFunctionIf>(nested_function, arguments, params);
    }
};


/** There are two cases: for single argument and variadic.
  * Code for single argument is much more efficient.
  */
template <bool result_is_nullable, bool serialize_flag>
class AggregateFunctionIfNullUnary final
    : public AggregateFunctionNullBase<result_is_nullable, serialize_flag,
        AggregateFunctionIfNullUnary<result_is_nullable, serialize_flag>>
{
private:
    size_t num_arguments;
    bool filter_is_nullable = false;
    bool filter_is_only_null = false;

    /// The name of the nested function, including combinators (i.e. *If)
    ///
    /// getName() from the nested_function cannot be used because in case of *If combinator
    /// with Nullable argument nested_function will point to the function without combinator.
    /// (I.e. sumIf(Nullable, 1) -> sum()), and distributed query processing will fail.
    ///
    /// And nested_function cannot point to the function with *If since
    /// due to optimization in the add() which pass only one column with the result,
    /// and so AggregateFunctionIf::add() cannot be called this way
    /// (it write to the last argument -- num_arguments-1).
    ///
    /// And to avoid extra level of indirection, the name of function is cached:
    ///
    ///     AggregateFunctionIfNullUnary::add -> [ AggregateFunctionIf::add -> ] AggregateFunctionSum::add
    String name;

    using Base = AggregateFunctionNullBase<result_is_nullable, serialize_flag,
        AggregateFunctionIfNullUnary<result_is_nullable, serialize_flag>>;

    bool singleFilter(const IColumn ** columns, size_t row_num) const
    {
        const IColumn * filter_column = columns[num_arguments - 1];

        if (filter_is_nullable)
        {
            const ColumnNullable * nullable_column = assert_cast<const ColumnNullable *>(filter_column);
            filter_column = nullable_column->getNestedColumnPtr().get();
            const UInt8 * filter_null_map = nullable_column->getNullMapData().data();

            return assert_cast<const ColumnUInt8 &>(*filter_column).getData()[row_num] && !filter_null_map[row_num];
        }

        return assert_cast<const ColumnUInt8 &>(*filter_column).getData()[row_num];
    }

public:
    String getName() const override
    {
        return name;
    }

    AggregateFunctionIfNullUnary(const String & name_, AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : Base(std::move(nested_function_), arguments, params)
        , num_arguments(arguments.size())
        , name(name_)
    {
        if (num_arguments == 0)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function {} require at least one argument", getName());

        filter_is_nullable = arguments[num_arguments - 1]->isNullable();
        filter_is_only_null = arguments[num_arguments - 1]->onlyNull();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (filter_is_only_null)
            return;

        const ColumnNullable * column = assert_cast<const ColumnNullable *>(columns[0]);
        const IColumn * nested_column = &column->getNestedColumn();
        if (!column->isNullAt(row_num) && singleFilter(columns, row_num))
        {
            this->setFlag(place);
            this->nested_function->add(this->nestedPlace(place), &nested_column, row_num, arena);
        }
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t) const override
    {
        if (filter_is_only_null)
            return;

        const ColumnNullable * column = assert_cast<const ColumnNullable *>(columns[0]);
        const UInt8 * null_map = column->getNullMapData().data();
        const IColumn * columns_param[] = {&column->getNestedColumn()};

        const IColumn * filter_column = columns[num_arguments - 1];

        const UInt8 * filter_values = nullptr;
        const UInt8 * filter_null_map = nullptr;

        if (filter_is_nullable)
        {
            const ColumnNullable * nullable_column = assert_cast<const ColumnNullable *>(filter_column);
            filter_column = nullable_column->getNestedColumnPtr().get();
            filter_null_map = nullable_column->getNullMapData().data();
        }

        filter_values = assert_cast<const ColumnUInt8 *>(filter_column)->getData().data();

        /// Combine the 2 flag arrays so we can call a simplified version (one check vs 2)
        /// Note that now the null map will contain 0 if not null and not filtered, or 1 for null or filtered (or both)

        auto final_nulls = std::make_unique<UInt8[]>(row_end);

        if (filter_null_map)
            for (size_t i = row_begin; i < row_end; ++i)
                final_nulls[i] = (!!null_map[i]) | (!filter_values[i]) | (!!filter_null_map[i]);
        else
            for (size_t i = row_begin; i < row_end; ++i)
                final_nulls[i] = (!!null_map[i]) | (!filter_values[i]);

        if constexpr (result_is_nullable)
        {
            if (!memoryIsByte(final_nulls.get(), row_begin, row_end, 1))
                this->setFlag(place);
            else
                return; /// No work to do.
        }

        this->nested_function->addBatchSinglePlaceNotNull(
            row_begin,
            row_end,
            this->nestedPlace(place),
            columns_param,
            final_nulls.get(),
            arena,
            -1);
    }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        return canBeNativeType(*this->argument_types.back()) && this->nested_function->isCompilable();
    }

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType & arguments) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        const auto & nullable_type = arguments[0].type;
        const auto & nullable_value = arguments[0].value;

        auto * wrapped_value = b.CreateExtractValue(nullable_value, {0});
        auto * is_null_value = b.CreateExtractValue(nullable_value, {1});

        const auto & predicate_type = arguments.back().type;
        auto * predicate_value = arguments.back().value;
        auto * is_predicate_true = nativeBoolCast(b, predicate_type, predicate_value);

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * if_null = llvm::BasicBlock::Create(head->getContext(), "if_null", head->getParent());
        auto * if_not_null = llvm::BasicBlock::Create(head->getContext(), "if_not_null", head->getParent());

        b.CreateCondBr(b.CreateAnd(b.CreateNot(is_null_value), is_predicate_true), if_not_null, if_null);

        b.SetInsertPoint(if_null);
        b.CreateBr(join_block);

        b.SetInsertPoint(if_not_null);

        if constexpr (result_is_nullable)
            b.CreateStore(llvm::ConstantInt::get(b.getInt8Ty(), 1), aggregate_data_ptr);

        auto * aggregate_data_ptr_with_prefix_size_offset = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_ptr, this->prefix_size);
        this->nested_function->compileAdd(b, aggregate_data_ptr_with_prefix_size_offset, { ValueWithType(wrapped_value, removeNullable(nullable_type)) });
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

#endif

};

template <bool result_is_nullable, bool serialize_flag>
class AggregateFunctionIfNullVariadic final : public AggregateFunctionNullBase<
                                                  result_is_nullable,
                                                  serialize_flag,
                                                  AggregateFunctionIfNullVariadic<result_is_nullable, serialize_flag>>
{
private:
    bool filter_is_only_null = false;

public:

    String getName() const override
    {
        return Base::getName();
    }

    AggregateFunctionIfNullVariadic(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : Base(std::move(nested_function_), arguments, params), number_of_arguments(arguments.size())
    {
        if (number_of_arguments == 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Single argument is passed to AggregateFunctionIfNullVariadic");

        if (number_of_arguments > MAX_ARGS)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Maximum number of arguments for aggregate function with Nullable types is {}", toString(MAX_ARGS));

        for (size_t i = 0; i < number_of_arguments; ++i)
            is_nullable[i] = arguments[i]->isNullable();

        filter_is_only_null = arguments.back()->onlyNull();
    }

    static bool singleFilter(const IColumn ** columns, size_t row_num, size_t num_arguments)
    {
        return assert_cast<const ColumnUInt8 &>(*columns[num_arguments - 1]).getData()[row_num];
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        /// This container stores the columns we really pass to the nested function.
        const IColumn * nested_columns[number_of_arguments];

        for (size_t i = 0; i < number_of_arguments; ++i)
        {
            if (is_nullable[i])
            {
                const ColumnNullable & nullable_col = assert_cast<const ColumnNullable &>(*columns[i]);
                if (nullable_col.isNullAt(row_num))
                {
                    /// If at least one column has a null value in the current row,
                    /// we don't process this row.
                    return;
                }
                nested_columns[i] = &nullable_col.getNestedColumn();
            }
            else
                nested_columns[i] = columns[i];
        }

        if (singleFilter(nested_columns, row_num, number_of_arguments))
        {
            this->setFlag(place);
            this->nested_function->add(this->nestedPlace(place), nested_columns, row_num, arena);
        }
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena * arena, ssize_t) const final
    {
        if (filter_is_only_null)
            return;

        std::unique_ptr<UInt8[]> final_null_flags = std::make_unique<UInt8[]>(row_end);
        const size_t filter_column_num = number_of_arguments - 1;

        if (is_nullable[filter_column_num])
        {
            const ColumnNullable * nullable_column = assert_cast<const ColumnNullable *>(columns[filter_column_num]);
            const IColumn & filter_column = nullable_column->getNestedColumn();
            const UInt8 * filter_null_map = nullable_column->getNullMapColumn().getData().data();
            const UInt8 * filter_values = assert_cast<const ColumnUInt8 &>(filter_column).getData().data();

            for (size_t i = row_begin; i < row_end; i++)
            {
                final_null_flags[i] = filter_null_map[i] || !filter_values[i];
            }
        }
        else
        {
            const IColumn * filter_column = columns[filter_column_num];
            const UInt8 * filter_values = assert_cast<const ColumnUInt8 *>(filter_column)->getData().data();
            for (size_t i = row_begin; i < row_end; i++)
                final_null_flags[i] = !filter_values[i];
        }

        const IColumn * nested_columns[number_of_arguments];
        for (size_t arg = 0; arg < number_of_arguments; arg++)
        {
            if (is_nullable[arg])
            {
                const ColumnNullable & nullable_col = assert_cast<const ColumnNullable &>(*columns[arg]);
                if (arg != filter_column_num)
                {
                    const ColumnUInt8 & nullmap_column = nullable_col.getNullMapColumn();
                    const UInt8 * col_null_map = nullmap_column.getData().data();
                    for (size_t r = row_begin; r < row_end; r++)
                    {
                        final_null_flags[r] |= col_null_map[r];
                    }
                }
                nested_columns[arg] = &nullable_col.getNestedColumn();
            }
            else
                nested_columns[arg] = columns[arg];
        }

        bool at_least_one = false;
        for (size_t i = row_begin; i < row_end; i++)
        {
            if (!final_null_flags[i])
            {
                at_least_one = true;
                break;
            }
        }

        if (at_least_one)
        {
            this->setFlag(place);
            this->nested_function->addBatchSinglePlaceNotNull(
                row_begin, row_end, this->nestedPlace(place), nested_columns, final_null_flags.get(), arena, -1);
        }
    }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        return canBeNativeType(*this->argument_types.back()) && this->nested_function->isCompilable();
    }

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType & arguments) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        size_t arguments_size = arguments.size();

        ValuesWithType wrapped_arguments;
        wrapped_arguments.reserve(arguments_size);

        std::vector<llvm::Value * > is_null_values;

        for (size_t i = 0; i < arguments_size; ++i)
        {
            const auto & argument_value = arguments[i].value;
            const auto & argument_type = arguments[i].type;

            if (is_nullable[i])
            {
                auto * wrapped_value = b.CreateExtractValue(argument_value, {0});
                is_null_values.emplace_back(b.CreateExtractValue(argument_value, {1}));
                wrapped_arguments.emplace_back(wrapped_value, removeNullable(argument_type));
            }
            else
            {
                wrapped_arguments.emplace_back(argument_value, argument_type);
            }
        }

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * join_block_after_null_checks = llvm::BasicBlock::Create(head->getContext(), "join_block_after_null_checks", head->getParent());

        auto * values_have_null_ptr = b.CreateAlloca(b.getInt1Ty());
        b.CreateStore(b.getInt1(false), values_have_null_ptr);

        for (auto * is_null_value : is_null_values)
        {
            auto * values_have_null = b.CreateLoad(b.getInt1Ty(), values_have_null_ptr);
            b.CreateStore(b.CreateOr(values_have_null, is_null_value), values_have_null_ptr);
        }

        b.CreateCondBr(b.CreateLoad(b.getInt1Ty(), values_have_null_ptr), join_block, join_block_after_null_checks);

        b.SetInsertPoint(join_block_after_null_checks);

        const auto & predicate_type = arguments.back().type;
        auto * predicate_value = arguments.back().value;
        auto * is_predicate_true = nativeBoolCast(b, predicate_type, predicate_value);

        auto * if_true = llvm::BasicBlock::Create(head->getContext(), "if_true", head->getParent());
        auto * if_false = llvm::BasicBlock::Create(head->getContext(), "if_false", head->getParent());

        b.CreateCondBr(is_predicate_true, if_true, if_false);

        b.SetInsertPoint(if_false);
        b.CreateBr(join_block);

        b.SetInsertPoint(if_true);

        if constexpr (result_is_nullable)
            b.CreateStore(llvm::ConstantInt::get(b.getInt8Ty(), 1), aggregate_data_ptr);

        auto * aggregate_data_ptr_with_prefix_size_offset = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_ptr, this->prefix_size);
        this->nested_function->compileAdd(b, aggregate_data_ptr_with_prefix_size_offset, wrapped_arguments);
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

#endif

private:
    using Base = AggregateFunctionNullBase<
        result_is_nullable,
        serialize_flag,
        AggregateFunctionIfNullVariadic<result_is_nullable, serialize_flag>>;

    static constexpr size_t MAX_ARGS = 8;
    size_t number_of_arguments = 0;
    std::array<char, MAX_ARGS> is_nullable;    /// Plain array is better than std::vector due to one indirection less.
};


AggregateFunctionPtr AggregateFunctionIf::getOwnNullAdapter(
    const AggregateFunctionPtr & nested_function, const DataTypes & arguments,
    const Array & params, const AggregateFunctionProperties & properties) const
{
    assert(!arguments.empty());

    /// Nullability of the last argument (condition) does not affect the nullability of the result (NULL is processed as false).
    /// For other arguments it is as usual (at least one is NULL then the result is NULL if possible).
    bool return_type_is_nullable = !properties.returns_default_when_only_null && getResultType()->canBeInsideNullable()
        && std::any_of(arguments.begin(), arguments.end() - 1, [](const auto & element) { return element->isNullable(); });

    bool need_to_serialize_flag = return_type_is_nullable || properties.returns_default_when_only_null;

    if (arguments.size() <= 2 && arguments.front()->isNullable())
    {
        if (return_type_is_nullable)
        {
            return std::make_shared<AggregateFunctionIfNullUnary<true, true>>(nested_function->getName(), nested_func, arguments, params);
        }

        if (need_to_serialize_flag)
            return std::make_shared<AggregateFunctionIfNullUnary<false, true>>(nested_function->getName(), nested_func, arguments, params);
        return std::make_shared<AggregateFunctionIfNullUnary<false, false>>(nested_function->getName(), nested_func, arguments, params);
    }

    if (return_type_is_nullable)
    {
        return std::make_shared<AggregateFunctionIfNullVariadic<true, true>>(nested_function, arguments, params);
    }

    if (need_to_serialize_flag)
        return std::make_shared<AggregateFunctionIfNullVariadic<false, true>>(nested_function, arguments, params);
    return std::make_shared<AggregateFunctionIfNullVariadic<false, false>>(nested_function, arguments, params);
}

void registerAggregateFunctionCombinatorIf(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorIf>());
}

}
