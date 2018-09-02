#include <Functions/FunctionsMiscellaneous.h>

#include <cmath>
#include <ext/range.h>
#include <ext/bit_cast.h>
#include <Poco/Net/DNS.h>
#include <Common/ClickHouseRevision.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnWithDictionary.h>
#include <Functions/FunctionHelpers.h>
#include <Common/UnicodeBar.h>
#include <Common/UTF8Helpers.h>
#include <Common/FieldVisitors.h>
#include <Common/AlignedBuffer.h>
#include <Common/config_version.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeWithDictionary.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NumberTraits.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/Set.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/IStorage.h>
#include <Common/typeid_cast.h>
#include <Storages/getStructureOfRemoteTable.h>
#include <Common/DNSResolver.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int FUNCTION_IS_SPECIAL;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TOO_SLOW;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int FUNCTION_THROW_IF_VALUE_IS_NON_ZERO;
}

/** Helper functions
  *
  * visibleWidth(x) - calculates the approximate width when outputting the value in a text (tab-separated) form to the console.
  *
  * toTypeName(x) - get the type name
  * blockSize() - get the block size
  * materialize(x) - materialize the constant
  * ignore(...) is a function that takes any arguments, and always returns 0.
  * sleep(seconds) - the specified number of seconds sleeps each block.
  *
  * in(x, set) - function for evaluating the IN
  * notIn(x, set) - and NOT IN.
  *
  * arrayJoin(arr) - a special function - it can not be executed directly;
  *                     is used only to get the result type of the corresponding expression.
  *
  * replicate(x, arr) - creates an array of the same size as arr, all elements of which are equal to x;
  *                  for example: replicate(1, ['a', 'b', 'c']) = [1, 1, 1].
  *
  * sleep(n) - sleeps n seconds for each block.
  *
  * bar(x, min, max, width) - draws a strip from the number of characters proportional to (x - min) and equal to width for x == max.
  *
  * version() - returns the current version of the server on the line.
  *
  * finalizeAggregation(agg_state) - get the result from the aggregation state.
  *
  * runningAccumulate(agg_state) - takes the states of the aggregate function and returns a column with values,
  * are the result of the accumulation of these states for a set of block lines, from the first to the current line.
  */


class FunctionCurrentDatabase : public IFunction
{
    const String db_name;

public:
    static constexpr auto name = "currentDatabase";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionCurrentDatabase>(context.getCurrentDatabase());
    }

    explicit FunctionCurrentDatabase(const String & db_name) : db_name{db_name}
    {
    }

    String getName() const override
    {
        return name;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, db_name);
    }
};


/// Get the host name. Is is constant on single server, but is not constant in distributed queries.
class FunctionHostName : public IFunction
{
public:
    static constexpr auto name = "hostName";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionHostName>();
    }

    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    /** convertToFullColumn needed because in distributed query processing,
      *    each server returns its own value.
      */
    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(
            input_rows_count, DNSResolver::instance().getHostName())->convertToFullColumnIfConst();
    }
};


class FunctionVisibleWidth : public IFunction
{
public:
    static constexpr auto name = "visibleWidth";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionVisibleWidth>();
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    /// Get the name of the function.
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    /// Execute the function on the block.
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override;
};


/// Returns name of IDataType instance (name of data type).
class FunctionToTypeName : public IFunction
{
public:
    static constexpr auto name = "toTypeName";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionToTypeName>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForColumnsWithDictionary() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    /// Execute the function on the block.
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column
            = DataTypeString().createColumnConst(input_rows_count, block.getByPosition(arguments[0]).type->getName());
    }
};


/// Returns number of fields in Enum data type of passed value.
class FunctionGetSizeOfEnumType : public IFunction
{
public:
    static constexpr auto name = "getSizeOfEnumType";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionGetSizeOfEnumType>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (checkDataType<DataTypeEnum8>(arguments[0].get()))
            return std::make_shared<DataTypeUInt8>();
        else if (checkDataType<DataTypeEnum16>(arguments[0].get()))
            return std::make_shared<DataTypeUInt16>();

        throw Exception("The argument for function " + getName() + " must be Enum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        if (auto type = checkAndGetDataType<DataTypeEnum8>(block.getByPosition(arguments[0]).type.get()))
            block.getByPosition(result).column = DataTypeUInt8().createColumnConst(input_rows_count, UInt64(type->getValues().size()));
        else if (auto type = checkAndGetDataType<DataTypeEnum16>(block.getByPosition(arguments[0]).type.get()))
            block.getByPosition(result).column = DataTypeUInt16().createColumnConst(input_rows_count, UInt64(type->getValues().size()));
        else
            throw Exception("The argument for function " + getName() + " must be Enum", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};



/// Returns name of IColumn instance.
class FunctionToColumnTypeName : public IFunction
{
public:
    static constexpr auto name = "toColumnTypeName";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionToColumnTypeName>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column
            = DataTypeString().createColumnConst(input_rows_count, block.getByPosition(arguments[0]).column->getName());
    }
};



/// Dump the structure of type and column.
class FunctionDumpColumnStructure : public IFunction
{
public:
    static constexpr auto name = "dumpColumnStructure";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionDumpColumnStructure>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const auto & elem = block.getByPosition(arguments[0]);

        /// Note that the result is not a constant, because it contains block size.

        block.getByPosition(result).column
            = DataTypeString().createColumnConst(input_rows_count,
                elem.type->getName() + ", " + elem.column->dumpStructure())->convertToFullColumnIfConst();
    }
};


/// Returns global default value for type of passed argument (example: 0 for numeric types, '' for String).
class FunctionDefaultValueOfArgumentType : public IFunction
{
public:
    static constexpr auto name = "defaultValueOfArgumentType";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionDefaultValueOfArgumentType>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return arguments[0];
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const IDataType & type = *block.getByPosition(arguments[0]).type;
        block.getByPosition(result).column = type.createColumnConst(input_rows_count, type.getDefault());
    }
};


class FunctionBlockSize : public IFunction
{
public:
    static constexpr auto name = "blockSize";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionBlockSize>();
    }

    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = ColumnUInt64::create(input_rows_count, input_rows_count);
    }
};


class FunctionRowNumberInBlock : public IFunction
{
public:
    static constexpr auto name = "rowNumberInBlock";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionRowNumberInBlock>();
    }

    /// Get the name of the function.
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        auto column = ColumnUInt64::create();
        auto & data = column->getData();
        data.resize(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i)
            data[i] = i;

        block.getByPosition(result).column = std::move(column);
    }
};


/** Incremental block number among calls of this function. */
class FunctionBlockNumber : public IFunction
{
private:
    std::atomic<size_t> block_number{0};

public:
    static constexpr auto name = "blockNumber";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionBlockNumber>();
    }

    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        size_t current_block_number = block_number++;
        block.getByPosition(result).column = ColumnUInt64::create(input_rows_count, current_block_number);
    }
};


/** Incremental number of row within all blocks passed to this function. */
class FunctionRowNumberInAllBlocks : public IFunction
{
private:
    std::atomic<size_t> rows{0};

public:
    static constexpr auto name = "rowNumberInAllBlocks";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionRowNumberInAllBlocks>();
    }

    /// Get the name of the function.
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        size_t current_row_number = rows.fetch_add(input_rows_count);

        auto column = ColumnUInt64::create();
        auto & data = column->getData();
        data.resize(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i)
            data[i] = current_row_number + i;

        block.getByPosition(result).column = std::move(column);
    }
};


enum class FunctionSleepVariant
{
    PerBlock,
    PerRow
};

template <FunctionSleepVariant variant>
class FunctionSleep : public IFunction
{
public:
    static constexpr auto name = variant == FunctionSleepVariant::PerBlock ? "sleep" : "sleepEachRow";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionSleep<variant>>();
    }

    /// Get the name of the function.
    String getName() const override
    {
        return name;
    }

    /// Do not sleep during query analysis.
    bool isSuitableForConstantFolding() const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!checkDataType<DataTypeFloat64>(arguments[0].get()) && !checkDataType<DataTypeFloat32>(arguments[0].get())
            && !checkDataType<DataTypeUInt64>(arguments[0].get())
            && !checkDataType<DataTypeUInt32>(arguments[0].get())
            && !checkDataType<DataTypeUInt16>(arguments[0].get())
            && !checkDataType<DataTypeUInt8>(arguments[0].get()))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName() + ", expected Float64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const IColumn * col = block.getByPosition(arguments[0]).column.get();

        if (!col->isColumnConst())
            throw Exception("The argument of function " + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);

        Float64 seconds = applyVisitor(FieldVisitorConvertToNumber<Float64>(), static_cast<const ColumnConst &>(*col).getField());

        if (seconds < 0)
            throw Exception("Cannot sleep negative amount of time (not implemented)", ErrorCodes::BAD_ARGUMENTS);

        size_t size = col->size();

        /// We do not sleep if the block is empty.
        if (size > 0)
        {
            unsigned useconds = seconds * (variant == FunctionSleepVariant::PerBlock ? 1 : size) * 1e6;

            /// When sleeping, the query cannot be cancelled. For abitily to cancel query, we limit sleep time.
            if (useconds > 3000000)   /// The choice is arbitary
                throw Exception("The maximum sleep time is 3000000 microseconds. Requested: " + toString(useconds), ErrorCodes::TOO_SLOW);

            usleep(useconds);
        }

        /// convertToFullColumn needed, because otherwise (constant expression case) function will not get called on each block.
        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(size, UInt64(0))->convertToFullColumnIfConst();
    }
};


class FunctionMaterialize : public IFunction
{
public:
    static constexpr auto name = "materialize";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionMaterialize>();
    }

    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return arguments[0];
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const auto & src = block.getByPosition(arguments[0]).column;
        if (ColumnPtr converted = src->convertToFullColumnIfConst())
            block.getByPosition(result).column = converted;
        else
            block.getByPosition(result).column = src;
    }
};

template <bool negative, bool global>
struct FunctionInName;
template <>
struct FunctionInName<false, false>
{
    static constexpr auto name = "in";
};
template <>
struct FunctionInName<false, true>
{
    static constexpr auto name = "globalIn";
};
template <>
struct FunctionInName<true, false>
{
    static constexpr auto name = "notIn";
};
template <>
struct FunctionInName<true, true>
{
    static constexpr auto name = "globalNotIn";
};

template <bool negative, bool global>
class FunctionIn : public IFunction
{
public:
    static constexpr auto name = FunctionInName<negative, global>::name;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionIn>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForNulls() const override
    {
        return false;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        /// Second argument must be ColumnSet.
        ColumnPtr column_set_ptr = block.getByPosition(arguments[1]).column;
        const ColumnSet * column_set = typeid_cast<const ColumnSet *>(&*column_set_ptr);
        if (!column_set)
            throw Exception("Second argument for function '" + getName() + "' must be Set; found " + column_set_ptr->getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        Block block_of_key_columns;

        /// First argument may be tuple or single column.
        const ColumnWithTypeAndName & left_arg = block.getByPosition(arguments[0]);
        const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(left_arg.column.get());
        const ColumnConst * const_tuple = checkAndGetColumnConst<ColumnTuple>(left_arg.column.get());
        const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(left_arg.type.get());

        ColumnPtr materialized_tuple;
        if (const_tuple)
        {
            materialized_tuple = const_tuple->convertToFullColumn();
            tuple = typeid_cast<const ColumnTuple *>(materialized_tuple.get());
        }

        auto set = column_set->getData();
        auto set_types = set->getDataTypes();
        if (tuple && (set_types.size() != 1 || !set_types[0]->equals(*type_tuple)))
        {
            const Columns & tuple_columns = tuple->getColumns();
            const DataTypes & tuple_types = type_tuple->getElements();
            size_t tuple_size = tuple_columns.size();
            for (size_t i = 0; i < tuple_size; ++i)
                block_of_key_columns.insert({ tuple_columns[i], tuple_types[i], "" });
        }
        else
            block_of_key_columns.insert(left_arg);

        block.getByPosition(result).column = set->execute(block_of_key_columns, negative);
    }
};


class FunctionIgnore : public IFunction
{
public:
    static constexpr auto name = "ignore";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionIgnore>();
    }

    bool isVariadic() const override
    {
        return true;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeUInt8().createColumnConst(input_rows_count, UInt64(0));
    }
};


/** The `indexHint` function takes any number of any arguments and always returns one.
  *
  * This function has a special meaning (see ExpressionAnalyzer, KeyCondition)
  * - the expressions inside it are not evaluated;
  * - but when analyzing the index (selecting ranges for reading), this function is treated the same way,
  *   as if instead of using it the expression itself would be.
  *
  * Example: WHERE something AND indexHint(CounterID = 34)
  * - do not read or calculate CounterID = 34, but select ranges in which the CounterID = 34 expression can be true.
  *
  * The function can be used for debugging purposes, as well as for (hidden from the user) query conversions.
  */
class FunctionIndexHint : public IFunction
{
public:
    static constexpr auto name = "indexHint";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionIndexHint>();
    }

    bool isVariadic() const override
    {
        return true;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    String getName() const override
    {
        return name;
    }
    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeUInt8().createColumnConst(input_rows_count, UInt64(1));
    }
};


class FunctionIdentity : public IFunction
{
public:
    static constexpr auto name = "identity";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionIdentity>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return arguments.front();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        block.getByPosition(result).column = block.getByPosition(arguments.front()).column;
    }
};


class FunctionArrayJoin : public IFunction
{
public:
    static constexpr auto name = "arrayJoin";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionArrayJoin>();
    }


    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    /** It could return many different values for single argument. */
    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * arr = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!arr)
            throw Exception("Argument for function " + getName() + " must be Array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arr->getNestedType();
    }

    void executeImpl(Block &, const ColumnNumbers &, size_t, size_t /*input_rows_count*/) override
    {
        throw Exception("Function " + getName() + " must not be executed directly.", ErrorCodes::FUNCTION_IS_SPECIAL);
    }

    /// Because of function cannot be executed directly.
    bool isSuitableForConstantFolding() const override
    {
        return false;
    }
};


FunctionPtr FunctionReplicate::create(const Context &)
{
    return std::make_shared<FunctionReplicate>();
}

DataTypePtr FunctionReplicate::getReturnTypeImpl(const DataTypes & arguments) const
{
    const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());
    if (!array_type)
        throw Exception("Second argument for function " + getName() + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<DataTypeArray>(arguments[0]);
}

void FunctionReplicate::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    ColumnPtr first_column = block.getByPosition(arguments[0]).column;

    const ColumnArray * array_column = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[1]).column.get());
    ColumnPtr temp_column;

    if (!array_column)
    {
        auto const_array_column = checkAndGetColumnConst<ColumnArray>(block.getByPosition(arguments[1]).column.get());
        if (!const_array_column)
            throw Exception("Unexpected column for replicate", ErrorCodes::ILLEGAL_COLUMN);
        temp_column = const_array_column->convertToFullColumn();
        array_column = checkAndGetColumn<ColumnArray>(temp_column.get());
    }

    block.getByPosition(result).column
        = ColumnArray::create(first_column->replicate(array_column->getOffsets()), array_column->getOffsetsPtr());
}

/** Returns a string with nice Unicode-art bar with resolution of 1/8 part of symbol.
  */
class FunctionBar : public IFunction
{
public:
    static constexpr auto name = "bar";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionBar>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3 && arguments.size() != 4)
            throw Exception("Function " + getName()
                    + " requires from 3 or 4 parameters: value, min_value, max_value, [max_width_of_bar = 80]. Passed "
                    + toString(arguments.size())
                    + ".",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!arguments[0]->isNumber() || !arguments[1]->isNumber() || !arguments[2]->isNumber()
            || (arguments.size() == 4 && !arguments[3]->isNumber()))
            throw Exception("All arguments for function " + getName() + " must be numeric.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        Int64 min = extractConstant<Int64>(block, arguments, 1, "Second"); /// The level at which the line has zero length.
        Int64 max = extractConstant<Int64>(block, arguments, 2, "Third"); /// The level at which the line has the maximum length.

        /// The maximum width of the bar in characters, by default.
        Float64 max_width = arguments.size() == 4 ? extractConstant<Float64>(block, arguments, 3, "Fourth") : 80;

        if (max_width < 1)
            throw Exception("Max_width argument must be >= 1.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (max_width > 1000)
            throw Exception("Too large max_width.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        const auto & src = *block.getByPosition(arguments[0]).column;

        auto res_column = ColumnString::create();

        if (executeNumber<UInt8>(src, *res_column, min, max, max_width) || executeNumber<UInt16>(src, *res_column, min, max, max_width)
            || executeNumber<UInt32>(src, *res_column, min, max, max_width)
            || executeNumber<UInt64>(src, *res_column, min, max, max_width)
            || executeNumber<Int8>(src, *res_column, min, max, max_width)
            || executeNumber<Int16>(src, *res_column, min, max, max_width)
            || executeNumber<Int32>(src, *res_column, min, max, max_width)
            || executeNumber<Int64>(src, *res_column, min, max, max_width)
            || executeNumber<Float32>(src, *res_column, min, max, max_width)
            || executeNumber<Float64>(src, *res_column, min, max, max_width))
        {
            block.getByPosition(result).column = std::move(res_column);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

private:
    template <typename T>
    T extractConstant(Block & block, const ColumnNumbers & arguments, size_t argument_pos, const char * which_argument) const
    {
        const auto & column = *block.getByPosition(arguments[argument_pos]).column;

        if (!column.isColumnConst())
            throw Exception(
                which_argument + String(" argument for function ") + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);

        return applyVisitor(FieldVisitorConvertToNumber<T>(), column[0]);
    }

    template <typename T>
    static void fill(const PaddedPODArray<T> & src,
        ColumnString::Chars_t & dst_chars,
        ColumnString::Offsets & dst_offsets,
        Int64 min,
        Int64 max,
        Float64 max_width)
    {
        size_t size = src.size();
        size_t current_offset = 0;

        dst_offsets.resize(size);
        dst_chars.reserve(size * (UnicodeBar::getWidthInBytes(max_width) + 1)); /// lines 0-terminated.

        for (size_t i = 0; i < size; ++i)
        {
            Float64 width = UnicodeBar::getWidth(src[i], min, max, max_width);
            size_t next_size = current_offset + UnicodeBar::getWidthInBytes(width) + 1;
            dst_chars.resize(next_size);
            UnicodeBar::render(width, reinterpret_cast<char *>(&dst_chars[current_offset]));
            current_offset = next_size;
            dst_offsets[i] = current_offset;
        }
    }

    template <typename T>
    static void fill(T src, String & dst_chars, Int64 min, Int64 max, Float64 max_width)
    {
        Float64 width = UnicodeBar::getWidth(src, min, max, max_width);
        dst_chars.resize(UnicodeBar::getWidthInBytes(width));
        UnicodeBar::render(width, dst_chars.data());
    }

    template <typename T>
    static bool executeNumber(const IColumn & src, ColumnString & dst, Int64 min, Int64 max, Float64 max_width)
    {
        if (const ColumnVector<T> * col = checkAndGetColumn<ColumnVector<T>>(&src))
        {
            fill(col->getData(), dst.getChars(), dst.getOffsets(), min, max, max_width);
            return true;
        }
        else
            return false;
    }
};


template <typename Impl>
class FunctionNumericPredicate : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionNumericPredicate>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments.front()->isNumber())
            throw Exception{"Argument for function " + getName() + " must be number", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const auto in = block.getByPosition(arguments.front()).column.get();

        if (   !execute<UInt8>(block, in, result)
            && !execute<UInt16>(block, in, result)
            && !execute<UInt32>(block, in, result)
            && !execute<UInt64>(block, in, result)
            && !execute<Int8>(block, in, result)
            && !execute<Int16>(block, in, result)
            && !execute<Int32>(block, in, result)
            && !execute<Int64>(block, in, result)
            && !execute<Float32>(block, in, result)
            && !execute<Float64>(block, in, result))
            throw Exception{"Illegal column " + in->getName() + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
    }

    template <typename T>
    bool execute(Block & block, const IColumn * in_untyped, const size_t result)
    {
        if (const auto in = checkAndGetColumn<ColumnVector<T>>(in_untyped))
        {
            const auto size = in->size();

            auto out = ColumnUInt8::create(size);

            const auto & in_data = in->getData();
            auto & out_data = out->getData();

            for (const auto i : ext::range(0, size))
                out_data[i] = Impl::execute(in_data[i]);

            block.getByPosition(result).column = std::move(out);
            return true;
        }

        return false;
    }
};

struct IsFiniteImpl
{
    /// Better implementation, because isinf, isfinite, isnan are not inlined for unknown reason.
    /// Assuming IEEE 754.
    /// NOTE gcc 7 doesn't vectorize this loop.

    static constexpr auto name = "isFinite";
    template <typename T>
    static bool execute(const T t)
    {
        if constexpr (std::is_same_v<T, float>)
            return (ext::bit_cast<uint32_t>(t)
                 & 0b01111111100000000000000000000000)
                != 0b01111111100000000000000000000000;
        else if constexpr (std::is_same_v<T, double>)
            return (ext::bit_cast<uint64_t>(t)
                 & 0b0111111111110000000000000000000000000000000000000000000000000000)
                != 0b0111111111110000000000000000000000000000000000000000000000000000;
        else
        {
            (void)t;
            return true;
        }
    }
};

struct IsInfiniteImpl
{
    static constexpr auto name = "isInfinite";
    template <typename T>
    static bool execute(const T t)
    {
        if constexpr (std::is_same_v<T, float>)
            return (ext::bit_cast<uint32_t>(t)
                 & 0b01111111111111111111111111111111)
                == 0b01111111100000000000000000000000;
        else if constexpr (std::is_same_v<T, double>)
            return (ext::bit_cast<uint64_t>(t)
                 & 0b0111111111111111111111111111111111111111111111111111111111111111)
                == 0b0111111111110000000000000000000000000000000000000000000000000000;
        else
        {
            (void)t;
            return false;
        }
    }
};

struct IsNaNImpl
{
    static constexpr auto name = "isNaN";
    template <typename T>
    static bool execute(const T t)
    {
        return t != t;  //-V501
    }
};

using FunctionIsFinite = FunctionNumericPredicate<IsFiniteImpl>;
using FunctionIsInfinite = FunctionNumericPredicate<IsInfiniteImpl>;
using FunctionIsNaN = FunctionNumericPredicate<IsNaNImpl>;


/** Returns server version (constant).
  */
class FunctionVersion : public IFunction
{
public:
    static constexpr auto name = "version";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionVersion>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        static const std::string version = getVersion();
        block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, version);
    }

private:
    std::string getVersion() const;
};


/** Returns server uptime in seconds.
  */
class FunctionUptime : public IFunction
{
public:
    static constexpr auto name = "uptime";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionUptime>(context.getUptimeSeconds());
    }

    explicit FunctionUptime(time_t uptime_) : uptime(uptime_)
    {
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt32>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeUInt32().createColumnConst(input_rows_count, static_cast<UInt64>(uptime));
    }

private:
    time_t uptime;
};


/** Returns the server time zone.
  */
class FunctionTimeZone : public IFunction
{
public:
    static constexpr auto name = "timezone";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionTimeZone>();
    }

    String getName() const override
    {
        return name;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, DateLUT::instance().getTimeZone());
    }
};


/** Quite unusual function.
  * Takes state of aggregate function (example runningAccumulate(uniqState(UserID))),
  *  and for each row of block, return result of aggregate function on merge of states of all previous rows and current row.
  *
  * So, result of function depends on partition of data to blocks and on order of data in block.
  */
class FunctionRunningAccumulate : public IFunction
{
public:
    static constexpr auto name = "runningAccumulate";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionRunningAccumulate>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isDeterministic() const override { return false; }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeAggregateFunction * type = checkAndGetDataType<DataTypeAggregateFunction>(arguments[0].get());
        if (!type)
            throw Exception("Argument for function " + getName() + " must have type AggregateFunction - state of aggregate function.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return type->getReturnType();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnAggregateFunction * column_with_states
            = typeid_cast<const ColumnAggregateFunction *>(&*block.getByPosition(arguments.at(0)).column);
        if (!column_with_states)
            throw Exception("Illegal column " + block.getByPosition(arguments.at(0)).column->getName()
                    + " of first argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        AggregateFunctionPtr aggregate_function_ptr = column_with_states->getAggregateFunction();
        const IAggregateFunction & agg_func = *aggregate_function_ptr;

        AlignedBuffer place(agg_func.sizeOfData(), agg_func.alignOfData());
        agg_func.create(place.data());
        SCOPE_EXIT(agg_func.destroy(place.data()));

        std::unique_ptr<Arena> arena = agg_func.allocatesMemoryInArena() ? std::make_unique<Arena>() : nullptr;

        auto result_column_ptr = agg_func.getReturnType()->createColumn();
        IColumn & result_column = *result_column_ptr;
        result_column.reserve(column_with_states->size());

        const auto & states = column_with_states->getData();
        for (const auto & state_to_add : states)
        {
            /// Will pass empty arena if agg_func does not allocate memory in arena
            agg_func.merge(place.data(), state_to_add, arena.get());
            agg_func.insertResultInto(place.data(), result_column);
        }

        block.getByPosition(result).column = std::move(result_column_ptr);
    }
};

template <bool is_first_line_zero>
struct FunctionRunningDifferenceName;

template <>
struct FunctionRunningDifferenceName<true>
{
    static constexpr auto name = "runningDifference";
};

template <>
struct FunctionRunningDifferenceName<false>
{
    static constexpr auto name = "runningDifferenceStartingWithFirstValue";
};

/** Calculate difference of consecutive values in block.
  * So, result of function depends on partition of data to blocks and on order of data in block.
  */
template <bool is_first_line_zero>
class FunctionRunningDifferenceImpl : public IFunction
{
private:
    /// It is possible to track value from previous block, to calculate continuously across all blocks. Not implemented.

    template <typename Src, typename Dst>
    static void process(const PaddedPODArray<Src> & src, PaddedPODArray<Dst> & dst, const NullMap * null_map)
    {
        size_t size = src.size();
        dst.resize(size);

        if (size == 0)
            return;

        /// It is possible to SIMD optimize this loop. By no need for that in practice.

        Src prev;
        bool has_prev_value = false;

        for (size_t i = 0; i < size; ++i)
        {
            if (null_map && (*null_map)[i])
                continue;

            if (!has_prev_value)
            {
                dst[i] = is_first_line_zero ? 0 : src[i];
                prev = src[i];
                has_prev_value = true;
            }
            else
            {
                auto cur = src[i];
                dst[i] = static_cast<Dst>(cur) - prev;
                prev = cur;
            }
        }
    }

    /// Result type is same as result of subtraction of argument types.
    template <typename SrcFieldType>
    using DstFieldType = typename NumberTraits::ResultOfSubtraction<SrcFieldType, SrcFieldType>::Type;

    /// Call polymorphic lambda with tag argument of concrete field type of src_type.
    template <typename F>
    void dispatchForSourceType(const IDataType & src_type, F && f) const
    {
        if (checkDataType<DataTypeUInt8>(&src_type))
            f(UInt8());
        else if (checkDataType<DataTypeUInt16>(&src_type))
            f(UInt16());
        else if (checkDataType<DataTypeUInt32>(&src_type))
            f(UInt32());
        else if (checkDataType<DataTypeUInt64>(&src_type))
            f(UInt64());
        else if (checkDataType<DataTypeInt8>(&src_type))
            f(Int8());
        else if (checkDataType<DataTypeInt16>(&src_type))
            f(Int16());
        else if (checkDataType<DataTypeInt32>(&src_type))
            f(Int32());
        else if (checkDataType<DataTypeInt64>(&src_type))
            f(Int64());
        else if (checkDataType<DataTypeFloat32>(&src_type))
            f(Float32());
        else if (checkDataType<DataTypeFloat64>(&src_type))
            f(Float64());
        else if (checkDataType<DataTypeDate>(&src_type))
            f(DataTypeDate::FieldType());
        else if (checkDataType<DataTypeDateTime>(&src_type))
            f(DataTypeDateTime::FieldType());
        else
            throw Exception("Argument for function " + getName() + " must have numeric type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

public:
    static constexpr auto name = FunctionRunningDifferenceName<is_first_line_zero>::name;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionRunningDifferenceImpl<is_first_line_zero>>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypePtr res;
        dispatchForSourceType(*removeNullable(arguments[0]), [&](auto field_type_tag)
        {
            res = std::make_shared<DataTypeNumber<DstFieldType<decltype(field_type_tag)>>>();
        });

        if (arguments[0]->isNullable())
            res = makeNullable(res);

        return res;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        auto & src = block.getByPosition(arguments.at(0));
        const auto & res_type = block.getByPosition(result).type;

        /// When column is constant, its difference is zero.
        if (src.column->isColumnConst())
        {
            block.getByPosition(result).column = res_type->createColumnConstWithDefaultValue(input_rows_count);
            return;
        }

        auto res_column = removeNullable(res_type)->createColumn();
        auto * src_column = src.column.get();
        ColumnPtr null_map_column = nullptr;
        const NullMap * null_map = nullptr;
        if (auto * nullable_column = checkAndGetColumn<ColumnNullable>(src_column))
        {
            src_column = &nullable_column->getNestedColumn();
            null_map_column = nullable_column->getNullMapColumnPtr();
            null_map = &nullable_column->getNullMapData();
        }

        dispatchForSourceType(*removeNullable(src.type), [&](auto field_type_tag)
        {
            using SrcFieldType = decltype(field_type_tag);

            process(static_cast<const ColumnVector<SrcFieldType> &>(*src_column).getData(),
                static_cast<ColumnVector<DstFieldType<SrcFieldType>> &>(*res_column).getData(), null_map);
        });

        if (null_map_column)
            block.getByPosition(result).column = ColumnNullable::create(std::move(res_column), null_map_column);
        else
            block.getByPosition(result).column = std::move(res_column);
    }
};

using FunctionRunningDifference = FunctionRunningDifferenceImpl<true>;
using FunctionRunningIncome = FunctionRunningDifferenceImpl<false>;


/** Takes state of aggregate function. Returns result of aggregation (finalized state).
  */
class FunctionFinalizeAggregation : public IFunction
{
public:
    static constexpr auto name = "finalizeAggregation";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionFinalizeAggregation>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeAggregateFunction * type = checkAndGetDataType<DataTypeAggregateFunction>(arguments[0].get());
        if (!type)
            throw Exception("Argument for function " + getName() + " must have type AggregateFunction - state of aggregate function.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return type->getReturnType();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnAggregateFunction * column_with_states
            = typeid_cast<const ColumnAggregateFunction *>(&*block.getByPosition(arguments.at(0)).column);
        if (!column_with_states)
            throw Exception("Illegal column " + block.getByPosition(arguments.at(0)).column->getName()
                    + " of first argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = column_with_states->convertToValues();
    }
};


/** Usage:
 *  hasColumnInTable(['hostname'[, 'username'[, 'password']],] 'database', 'table', 'column')
 */
class FunctionHasColumnInTable : public IFunction
{
public:
    static constexpr auto name = "hasColumnInTable";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionHasColumnInTable>(context.getGlobalContext());
    }

    explicit FunctionHasColumnInTable(const Context & global_context_) : global_context(global_context_)
    {
    }

    bool isVariadic() const override
    {
        return true;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

private:
    const Context & global_context;
};


void FunctionVisibleWidth::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
{
    auto & src = block.getByPosition(arguments[0]);
    size_t size = input_rows_count;

    auto res_col = ColumnUInt64::create(size);
    auto & res_data = static_cast<ColumnUInt64 &>(*res_col).getData();

    /// For simplicity reasons, function is implemented by serializing into temporary buffer.

    String tmp;
    FormatSettings format_settings;
    for (size_t i = 0; i < size; ++i)
    {
        {
            WriteBufferFromString out(tmp);
            src.type->serializeText(*src.column, i, out, format_settings);
        }

        res_data[i] = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(tmp.data()), tmp.size());
    }

    block.getByPosition(result).column = std::move(res_col);
}


DataTypePtr FunctionHasColumnInTable::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    if (arguments.size() < 3 || arguments.size() > 6)
        throw Exception{"Invalid number of arguments for function " + getName(),
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    static const std::string arg_pos_description[] = {"First", "Second", "Third", "Fourth", "Fifth", "Sixth"};
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        const ColumnWithTypeAndName & argument = arguments[i];

        if (!checkColumnConst<ColumnString>(argument.column.get()))
        {
            throw Exception(arg_pos_description[i] + " argument for function " + getName() + " must be const String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    return std::make_shared<DataTypeUInt8>();
}


void FunctionHasColumnInTable::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
{
    auto get_string_from_block = [&](size_t column_pos) -> String
    {
        ColumnPtr column = block.getByPosition(column_pos).column;
        const ColumnConst * const_column = checkAndGetColumnConst<ColumnString>(column.get());
        return const_column->getValue<String>();
    };

    size_t arg = 0;
    String host_name;
    String user_name;
    String password;

    if (arguments.size() > 3)
        host_name = get_string_from_block(arguments[arg++]);

    if (arguments.size() > 4)
        user_name = get_string_from_block(arguments[arg++]);

    if (arguments.size() > 5)
        password = get_string_from_block(arguments[arg++]);

    String database_name = get_string_from_block(arguments[arg++]);
    String table_name = get_string_from_block(arguments[arg++]);
    String column_name = get_string_from_block(arguments[arg++]);

    bool has_column;
    if (host_name.empty())
    {
        const StoragePtr & table = global_context.getTable(database_name, table_name);
        has_column = table->hasColumn(column_name);
    }
    else
    {
        std::vector<std::vector<String>> host_names = {{ host_name }};
        auto cluster = std::make_shared<Cluster>(global_context.getSettings(), host_names, !user_name.empty() ? user_name : "default", password, global_context.getTCPPort(), false);
        auto remote_columns = getStructureOfRemoteTable(*cluster, database_name, table_name, global_context);
        has_column = remote_columns.hasPhysical(column_name);
    }

    block.getByPosition(result).column = DataTypeUInt8().createColumnConst(input_rows_count, UInt64(has_column));
}


/// Throw an exception if the argument is non zero.
class FunctionThrowIf : public IFunction
{
public:
    static constexpr auto name = "throwIf";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionThrowIf>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments.front()->isNumber())
            throw Exception{"Argument for function " + getName() + " must be number", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const auto in = block.getByPosition(arguments.front()).column.get();

        if (   !execute<UInt8>(block, in, result)
            && !execute<UInt16>(block, in, result)
            && !execute<UInt32>(block, in, result)
            && !execute<UInt64>(block, in, result)
            && !execute<Int8>(block, in, result)
            && !execute<Int16>(block, in, result)
            && !execute<Int32>(block, in, result)
            && !execute<Int64>(block, in, result)
            && !execute<Float32>(block, in, result)
            && !execute<Float64>(block, in, result))
            throw Exception{"Illegal column " + in->getName() + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
    }

    template <typename T>
    bool execute(Block & block, const IColumn * in_untyped, const size_t result)
    {
        if (const auto in = checkAndGetColumn<ColumnVector<T>>(in_untyped))
        {
            const auto & in_data = in->getData();
            if (!memoryIsZero(in_data.data(), in_data.size() * sizeof(in_data[0])))
                throw Exception("Value passed to 'throwIf' function is non zero", ErrorCodes::FUNCTION_THROW_IF_VALUE_IS_NON_ZERO);

            /// We return non constant to avoid constant folding.
            block.getByPosition(result).column = ColumnUInt8::create(in_data.size(), 0);
            return true;
        }

        return false;
    }
};


class FunctionToLowCardinality: public IFunction
{
public:
    static constexpr auto name = "toLowCardinality";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToLowCardinality>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForColumnsWithDictionary() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->withDictionary())
            return arguments[0];

        return std::make_shared<DataTypeWithDictionary>(arguments[0]);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        auto arg_num = arguments[0];
        const auto & arg = block.getByPosition(arg_num);
        auto & res = block.getByPosition(result);

        if (arg.type->withDictionary())
            res.column = arg.column;
        else
        {
            auto column = res.type->createColumn();
            typeid_cast<ColumnWithDictionary &>(*column).insertRangeFromFullColumn(*arg.column, 0, arg.column->size());
            res.column = std::move(column);
        }
    }
};

class FunctionLowCardinalityIndexes: public IFunction
{
public:
    static constexpr auto name = "lowCardinalityIndexes";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionLowCardinalityIndexes>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForColumnsWithDictionary() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto * type = typeid_cast<const DataTypeWithDictionary *>(arguments[0].get());
        if (!type)
            throw Exception("First first argument of function lowCardinalityIndexes must be ColumnWithDictionary, but got"
                            + arguments[0]->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        auto arg_num = arguments[0];
        const auto & arg = block.getByPosition(arg_num);
        auto & res = block.getByPosition(result);
        auto indexes_col = typeid_cast<const ColumnWithDictionary *>(arg.column.get())->getIndexesPtr();
        auto new_indexes_col = ColumnUInt64::create(indexes_col->size());
        auto & data = new_indexes_col->getData();
        for (size_t i = 0; i < data.size(); ++i)
            data[i] = indexes_col->getUInt(i);

        res.column = std::move(new_indexes_col);
    }
};

class FunctionLowCardinalityKeys: public IFunction
{
public:
    static constexpr auto name = "lowCardinalityKeys";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionLowCardinalityKeys>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForColumnsWithDictionary() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto * type = typeid_cast<const DataTypeWithDictionary *>(arguments[0].get());
        if (!type)
            throw Exception("First first argument of function lowCardinalityKeys must be ColumnWithDictionary, but got"
                            + arguments[0]->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return type->getDictionaryType();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        auto arg_num = arguments[0];
        const auto & arg = block.getByPosition(arg_num);
        auto & res = block.getByPosition(result);
        const auto * column_with_dictionary = typeid_cast<const ColumnWithDictionary *>(arg.column.get());
        res.column = column_with_dictionary->getDictionary().getNestedColumn()->cloneResized(arg.column->size());
    }
};


std::string FunctionVersion::getVersion() const
{
    return VERSION_STRING;
}


void registerFunctionsMiscellaneous(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCurrentDatabase>();
    factory.registerFunction<FunctionHostName>();
    factory.registerFunction<FunctionVisibleWidth>();
    factory.registerFunction<FunctionToTypeName>();
    factory.registerFunction<FunctionGetSizeOfEnumType>();
    factory.registerFunction<FunctionToColumnTypeName>();
    factory.registerFunction<FunctionDumpColumnStructure>();
    factory.registerFunction<FunctionDefaultValueOfArgumentType>();
    factory.registerFunction<FunctionBlockSize>();
    factory.registerFunction<FunctionBlockNumber>();
    factory.registerFunction<FunctionRowNumberInBlock>();
    factory.registerFunction<FunctionRowNumberInAllBlocks>();
    factory.registerFunction<FunctionSleep<FunctionSleepVariant::PerBlock>>();
    factory.registerFunction<FunctionSleep<FunctionSleepVariant::PerRow>>();
    factory.registerFunction<FunctionMaterialize>();
    factory.registerFunction<FunctionIgnore>();
    factory.registerFunction<FunctionIndexHint>();
    factory.registerFunction<FunctionIdentity>();
    factory.registerFunction<FunctionArrayJoin>();
    factory.registerFunction<FunctionReplicate>();
    factory.registerFunction<FunctionBar>();
    factory.registerFunction<FunctionHasColumnInTable>();

    factory.registerFunction<FunctionIn<false, false>>();
    factory.registerFunction<FunctionIn<false, true>>();
    factory.registerFunction<FunctionIn<true, false>>();
    factory.registerFunction<FunctionIn<true, true>>();

    factory.registerFunction<FunctionIsFinite>();
    factory.registerFunction<FunctionIsInfinite>();
    factory.registerFunction<FunctionIsNaN>();
    factory.registerFunction<FunctionThrowIf>();

    factory.registerFunction<FunctionVersion>();
    factory.registerFunction<FunctionUptime>();
    factory.registerFunction<FunctionTimeZone>();

    factory.registerFunction<FunctionRunningAccumulate>();
    factory.registerFunction<FunctionRunningDifference>();
    factory.registerFunction<FunctionRunningIncome>();
    factory.registerFunction<FunctionFinalizeAggregation>();

    factory.registerFunction<FunctionToLowCardinality>();
    factory.registerFunction<FunctionLowCardinalityIndexes>();
    factory.registerFunction<FunctionLowCardinalityKeys>();
}
}
