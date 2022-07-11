#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/AggregateFunctionIf.h>
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
            throw Exception("Incorrect number of arguments for aggregate function with " + getName() + " suffix",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isUInt8(arguments.back()))
            throw Exception("Illegal type " + arguments.back()->getName() + " of last argument for aggregate function with " + getName() + " suffix",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

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

    inline bool singleFilter(const IColumn ** columns, size_t row_num) const
    {
        const IColumn * filter_column = columns[num_arguments - 1];

        if (filter_is_nullable)
        {
            const ColumnNullable * nullable_column = assert_cast<const ColumnNullable *>(filter_column);
            filter_column = nullable_column->getNestedColumnPtr().get();
            const UInt8 * filter_null_map = nullable_column->getNullMapData().data();

            return assert_cast<const ColumnUInt8 &>(*filter_column).getData()[row_num] && !filter_null_map[row_num];
        }
        else
        {
            return assert_cast<const ColumnUInt8 &>(*filter_column).getData()[row_num];
        }
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
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const ColumnNullable * column = assert_cast<const ColumnNullable *>(columns[0]);
        const IColumn * nested_column = &column->getNestedColumn();
        if (!column->isNullAt(row_num) && singleFilter(columns, row_num))
        {
            this->setFlag(place);
            this->nested_function->add(this->nestedPlace(place), &nested_column, row_num, arena);
        }
    }

    void addBatchSinglePlace(size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t) const override
    {
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

        auto final_nulls = std::make_unique<UInt8[]>(batch_size);

        if (filter_null_map)
            for (size_t i = 0; i < batch_size; ++i)
                final_nulls[i] = (!!null_map[i]) | (!filter_values[i]) | (!!filter_null_map[i]);
        else
            for (size_t i = 0; i < batch_size; ++i)
                final_nulls[i] = (!!null_map[i]) | (!filter_values[i]);

        if constexpr (result_is_nullable)
        {
            if (!memoryIsByte(final_nulls.get(), batch_size, 1))
                this->setFlag(place);
            else
                return; /// No work to do.
        }

        this->nested_function->addBatchSinglePlaceNotNull(
            batch_size, this->nestedPlace(place), columns_param, final_nulls.get(), arena, -1);
    }

#if USE_EMBEDDED_COMPILER

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const DataTypes & arguments_types, const std::vector<llvm::Value *> & argument_values) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        const auto & nullable_type = arguments_types[0];
        const auto & nullable_value = argument_values[0];

        auto * wrapped_value = b.CreateExtractValue(nullable_value, {0});
        auto * is_null_value = b.CreateExtractValue(nullable_value, {1});

        const auto & predicate_type = arguments_types[argument_values.size() - 1];
        auto * predicate_value = argument_values[argument_values.size() - 1];
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

        auto * aggregate_data_ptr_with_prefix_size_offset = b.CreateConstInBoundsGEP1_64(nullptr, aggregate_data_ptr, this->prefix_size);
        this->nested_function->compileAdd(b, aggregate_data_ptr_with_prefix_size_offset, { removeNullable(nullable_type) }, { wrapped_value });
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

#endif

};

template <bool result_is_nullable, bool serialize_flag, bool null_is_skipped>
class AggregateFunctionIfNullVariadic final
    : public AggregateFunctionNullBase<result_is_nullable, serialize_flag,
        AggregateFunctionIfNullVariadic<result_is_nullable, serialize_flag, null_is_skipped>>
{
public:

    String getName() const override
    {
        return Base::getName();
    }

    AggregateFunctionIfNullVariadic(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : Base(std::move(nested_function_), arguments, params), number_of_arguments(arguments.size())
    {
        if (number_of_arguments == 1)
            throw Exception("Logical error: single argument is passed to AggregateFunctionIfNullVariadic", ErrorCodes::LOGICAL_ERROR);

        if (number_of_arguments > MAX_ARGS)
            throw Exception("Maximum number of arguments for aggregate function with Nullable types is " + toString(size_t(MAX_ARGS)),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < number_of_arguments; ++i)
            is_nullable[i] = arguments[i]->isNullable();
    }

    static inline bool singleFilter(const IColumn ** columns, size_t row_num, size_t num_arguments)
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
                if (null_is_skipped && nullable_col.isNullAt(row_num))
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

#if USE_EMBEDDED_COMPILER

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const DataTypes & arguments_types, const std::vector<llvm::Value *> & argument_values) const override
    {
        /// TODO: Check

        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        size_t arguments_size = arguments_types.size();

        DataTypes non_nullable_types;
        std::vector<llvm::Value * > wrapped_values;
        std::vector<llvm::Value * > is_null_values;

        non_nullable_types.resize(arguments_size);
        wrapped_values.resize(arguments_size);
        is_null_values.resize(arguments_size);

        for (size_t i = 0; i < arguments_size; ++i)
        {
            const auto & argument_value = argument_values[i];

            if (is_nullable[i])
            {
                auto * wrapped_value = b.CreateExtractValue(argument_value, {0});

                if constexpr (null_is_skipped)
                    is_null_values[i] = b.CreateExtractValue(argument_value, {1});

                wrapped_values[i] = wrapped_value;
                non_nullable_types[i] = removeNullable(arguments_types[i]);
            }
            else
            {
                wrapped_values[i] = argument_value;
                non_nullable_types[i] = arguments_types[i];
            }
        }

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * join_block_after_null_checks = llvm::BasicBlock::Create(head->getContext(), "join_block_after_null_checks", head->getParent());

        if constexpr (null_is_skipped)
        {
            auto * values_have_null_ptr = b.CreateAlloca(b.getInt1Ty());
            b.CreateStore(b.getInt1(false), values_have_null_ptr);

            for (auto * is_null_value : is_null_values)
            {
                if (!is_null_value)
                    continue;

                auto * values_have_null = b.CreateLoad(b.getInt1Ty(), values_have_null_ptr);
                b.CreateStore(b.CreateOr(values_have_null, is_null_value), values_have_null_ptr);
            }

            b.CreateCondBr(b.CreateLoad(b.getInt1Ty(), values_have_null_ptr), join_block, join_block_after_null_checks);
        }

        b.SetInsertPoint(join_block_after_null_checks);

        const auto & predicate_type = arguments_types[argument_values.size() - 1];
        auto * predicate_value = argument_values[argument_values.size() - 1];
        auto * is_predicate_true = nativeBoolCast(b, predicate_type, predicate_value);

        auto * if_true = llvm::BasicBlock::Create(head->getContext(), "if_true", head->getParent());
        auto * if_false = llvm::BasicBlock::Create(head->getContext(), "if_false", head->getParent());

        b.CreateCondBr(is_predicate_true, if_true, if_false);

        b.SetInsertPoint(if_false);
        b.CreateBr(join_block);

        b.SetInsertPoint(if_true);

        if constexpr (result_is_nullable)
            b.CreateStore(llvm::ConstantInt::get(b.getInt8Ty(), 1), aggregate_data_ptr);

        auto * aggregate_data_ptr_with_prefix_size_offset = b.CreateConstInBoundsGEP1_64(nullptr, aggregate_data_ptr, this->prefix_size);
        this->nested_function->compileAdd(b, aggregate_data_ptr_with_prefix_size_offset, non_nullable_types, wrapped_values);
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

#endif

private:
    using Base = AggregateFunctionNullBase<result_is_nullable, serialize_flag,
        AggregateFunctionIfNullVariadic<result_is_nullable, serialize_flag, null_is_skipped>>;

    enum { MAX_ARGS = 8 };
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
    bool return_type_is_nullable = !properties.returns_default_when_only_null && getReturnType()->canBeInsideNullable()
        && std::any_of(arguments.begin(), arguments.end() - 1, [](const auto & element) { return element->isNullable(); });

    bool need_to_serialize_flag = return_type_is_nullable || properties.returns_default_when_only_null;

    if (arguments.size() <= 2 && arguments.front()->isNullable())
    {
        if (return_type_is_nullable)
        {
            return std::make_shared<AggregateFunctionIfNullUnary<true, true>>(nested_function->getName(), nested_func, arguments, params);
        }
        else
        {
            if (need_to_serialize_flag)
                return std::make_shared<AggregateFunctionIfNullUnary<false, true>>(nested_function->getName(), nested_func, arguments, params);
            else
                return std::make_shared<AggregateFunctionIfNullUnary<false, false>>(nested_function->getName(), nested_func, arguments, params);
        }
    }
    else
    {
        if (return_type_is_nullable)
        {
            return std::make_shared<AggregateFunctionIfNullVariadic<true, true, true>>(nested_function, arguments, params);
        }
        else
        {
            if (need_to_serialize_flag)
                return std::make_shared<AggregateFunctionIfNullVariadic<false, true, true>>(nested_function, arguments, params);
            else
                return std::make_shared<AggregateFunctionIfNullVariadic<false, false, true>>(nested_function, arguments, params);
        }
    }
}

void registerAggregateFunctionCombinatorIf(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorIf>());
}

}
