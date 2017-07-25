#include <Functions/FunctionsMiscellaneous.h>

#include <cmath>
#include <ext/range.h>
#include <Poco/Net/DNS.h>
#include <Common/ClickHouseRevision.h>
#include <Columns/ColumnSet.h>
#include <Common/UnicodeBar.h>
#include <Common/UTF8Helpers.h>
#include <Core/FieldVisitors.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/Set.h>
#include <Storages/IStorage.h>
#include <Common/typeid_cast.h>
#include <TableFunctions/getStructureOfRemoteTable.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_INDEX;
    extern const int FUNCTION_IS_SPECIAL;
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
  * tuple(x, y, ...) is a function that allows you to group several columns
  * tupleElement(tuple, n) is a function that allows you to retrieve a column from tuple.
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        block.getByPosition(result).column = DataTypeString().createConstColumn(block.rows(), db_name);
    }
};


/// Get the host name. Is is constant on single server, but is not constant in distributed queries.
class FunctionHostName : public IFunction
{
public:
    static constexpr auto name = "hostName";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionHostName>();
    }

    String getName() const override
    {
        return name;
    }

    bool isDeterministicInScopeOfQuery() override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeString>();
    }

    /** convertToFullColumn needed because in distributed query processing,
      *    each server returns its own value.
      */
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        block.getByPosition(result).column = block.getByPosition(result).type->createConstColumn(
            block.rows(), Poco::Net::DNS::hostName())->convertToFullColumnIfConst();
    }
};


class FunctionVisibleWidth : public IFunction
{
public:
    static constexpr auto name = "visibleWidth";
    static FunctionPtr create(const Context & context)
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    /// Execute the function on the block.
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


/// Returns name of IDataType instance (name of data type).
class FunctionToTypeName : public IFunction
{
public:
    static constexpr auto name = "toTypeName";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionToTypeName>();
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
        return std::make_shared<DataTypeString>();
    }

    /// Execute the function on the block.
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        block.getByPosition(result).column
            = DataTypeString().createConstColumn(block.rows(), block.getByPosition(arguments[0]).type->getName());
    }
};


/// Returns name of IColumn instance.
class FunctionToColumnTypeName : public IFunction
{
public:
    static constexpr auto name = "toColumnTypeName";
    static FunctionPtr create(const Context & context)
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        block.getByPosition(result).column
            = DataTypeString().createConstColumn(block.rows(), block.getByPosition(arguments[0]).column->getName());
    }
};


/// Returns global default value for type of passed argument (example: 0 for numeric types, '' for String).
class FunctionDefaultValueOfArgumentType : public IFunction
{
public:
    static constexpr auto name = "defaultValueOfArgumentType";
    static FunctionPtr create(const Context & context)
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
        return arguments[0]->clone();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        IDataType & type = *block.getByPosition(arguments[0]).type;
        block.getByPosition(result).column = type.createConstColumn(block.rows(), type.getDefault());
    }
};


class FunctionBlockSize : public IFunction
{
public:
    static constexpr auto name = "blockSize";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionBlockSize>();
    }

    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    bool isDeterministicInScopeOfQuery() override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        size_t size = block.rows();
        block.getByPosition(result).column = std::make_shared<ColumnUInt64>(size, size);
    }
};


class FunctionRowNumberInBlock : public IFunction
{
public:
    static constexpr auto name = "rowNumberInBlock";
    static FunctionPtr create(const Context & context)
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

    bool isDeterministicInScopeOfQuery() override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        size_t size = block.rows();
        auto column = std::make_shared<ColumnUInt64>();
        auto & data = column->getData();
        data.resize(size);
        for (size_t i = 0; i < size; ++i)
            data[i] = i;

        block.getByPosition(result).column = column;
    }
};


/** Incremental block number among calls of this function. */
class FunctionBlockNumber : public IFunction
{
private:
    std::atomic<size_t> block_number{0};

public:
    static constexpr auto name = "blockNumber";
    static FunctionPtr create(const Context & context)
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

    bool isDeterministicInScopeOfQuery() override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        size_t current_block_number = block_number++;
        block.getByPosition(result).column = std::make_shared<ColumnUInt64>(block.rows(), current_block_number);
    }
};


/** Incremental number of row within all blocks passed to this function. */
class FunctionRowNumberInAllBlocks : public IFunction
{
private:
    std::atomic<size_t> rows{0};

public:
    static constexpr auto name = "rowNumberInAllBlocks";
    static FunctionPtr create(const Context & context)
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

    bool isDeterministicInScopeOfQuery() override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        size_t rows_in_block = block.rows();
        size_t current_row_number = rows.fetch_add(rows_in_block);

        auto column = std::make_shared<ColumnUInt64>();
        auto & data = column->getData();
        data.resize(rows_in_block);
        for (size_t i = 0; i < rows_in_block; ++i)
            data[i] = current_row_number + i;

        block.getByPosition(result).column = column;
    }
};


class FunctionSleep : public IFunction
{
public:
    static constexpr auto name = "sleep";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionSleep>();
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
        if (!checkDataType<DataTypeFloat64>(&*arguments[0]) && !checkDataType<DataTypeFloat32>(&*arguments[0])
            && !checkDataType<DataTypeUInt64>(&*arguments[0])
            && !checkDataType<DataTypeUInt32>(&*arguments[0])
            && !checkDataType<DataTypeUInt16>(&*arguments[0])
            && !checkDataType<DataTypeUInt8>(&*arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName() + ", expected Float64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        IColumn * col = block.getByPosition(arguments[0]).column.get();
        double seconds;
        size_t size = col->size();

        if (auto column = checkAndGetColumnConst<ColumnVector<Float64>>(col))
            seconds = column->getValue<Float64>();

        else if (auto column = checkAndGetColumnConst<ColumnVector<Float32>>(col))
            seconds = static_cast<double>(column->getValue<Float64>());

        else if (auto column = checkAndGetColumnConst<ColumnVector<UInt64>>(col))
            seconds = static_cast<double>(column->getValue<UInt64>());

        else if (auto column = checkAndGetColumnConst<ColumnVector<UInt32>>(col))
            seconds = static_cast<double>(column->getValue<UInt32>());

        else if (auto column = checkAndGetColumnConst<ColumnVector<UInt16>>(col))
            seconds = static_cast<double>(column->getValue<UInt16>());

        else if (auto column = checkAndGetColumnConst<ColumnVector<UInt8>>(col))
            seconds = static_cast<double>(column->getValue<UInt8>());

        else
            throw Exception("The argument of function " + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);

        /// We do not sleep if the block is empty.
        if (size > 0)
            usleep(static_cast<unsigned>(seconds * 1e6));

        /// convertToFullColumn needed, because otherwise (constant expression case) function will not get called on each block.
        block.getByPosition(result).column = block.getByPosition(result).type->createConstColumn(size, UInt64(0))->convertToFullColumnIfConst();
    }
};


class FunctionMaterialize : public IFunction
{
public:
    static constexpr auto name = "materialize";
    static FunctionPtr create(const Context & context)
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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        const auto & src = block.getByPosition(arguments[0]).column;
        if (auto converted = src->convertToFullColumnIfConst())
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
    static FunctionPtr create(const Context & context)
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        /// Second argument must be ColumnSet.
        ColumnPtr column_set_ptr = block.getByPosition(arguments[1]).column;
        const ColumnSet * column_set = typeid_cast<const ColumnSet *>(&*column_set_ptr);
        if (!column_set)
            throw Exception("Second argument for function '" + getName() + "' must be Set; found " + column_set_ptr->getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        Block block_of_key_columns;

        /// First argument may be tuple or single column.
        const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(block.getByPosition(arguments[0]).column.get());
        const ColumnConst * const_tuple = checkAndGetColumnConst<ColumnTuple>(block.getByPosition(arguments[0]).column.get());

        if (tuple)
            block_of_key_columns = tuple->getData();
        else if (const_tuple)
            block_of_key_columns = static_cast<const ColumnTuple &>(*const_tuple->convertToFullColumn()).getData();
        else
            block_of_key_columns.insert(block.getByPosition(arguments[0]));

        block.getByPosition(result).column = column_set->getData()->execute(block_of_key_columns, negative);
    }
};

FunctionPtr FunctionTuple::create(const Context & context)
{
    return std::make_shared<FunctionTuple>();
}

DataTypePtr FunctionTuple::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() < 1)
        throw Exception("Function " + getName() + " requires at least one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<DataTypeTuple>(arguments);
}

void FunctionTuple::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    Block tuple_block;

    size_t num_constants = 0;
    for (auto column_number : arguments)
    {
        const auto & elem = block.getByPosition(column_number);
        if (elem.column->isConst())
            ++num_constants;

        tuple_block.insert(elem);
    }

    if (num_constants == arguments.size())
    {
        /** Return ColumnConst rather than ColumnTuple of nested const columns.
          * (otherwise, ColumnTuple will not be understanded as constant in many places in code).
          */

        TupleBackend tuple(arguments.size());
        for (size_t i = 0, size = arguments.size(); i < size; ++i)
            tuple_block.getByPosition(i).column->get(0, tuple[i]);

        block.getByPosition(result).column
            = block.getByPosition(result).type->createConstColumn(block.rows(), Tuple(tuple));
    }
    else
    {
        ColumnPtr res = std::make_shared<ColumnTuple>(tuple_block);

        /** If tuple is mixed of constant and not constant columns,
              *  convert all to non-constant columns,
              *  because many places in code expect all non-constant columns in non-constant tuple.
              */
        if (num_constants != 0)
            if (auto converted = res->convertToFullColumnIfConst())
                res = converted;

        block.getByPosition(result).column = res;
    }
}


/** Extract element of tuple by constant index. The operation is essentially free.
  */
class FunctionTupleElement : public IFunction
{
public:
    static constexpr auto name = "tupleElement";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionTupleElement>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    void getReturnTypeAndPrerequisitesImpl(
        const ColumnsWithTypeAndName & arguments, DataTypePtr & out_return_type, ExpressionActions::Actions & out_prerequisites) override
    {
        auto index_col = checkAndGetColumnConst<ColumnUInt8>(&*arguments[1].column);
        if (!index_col)
            throw Exception("Second argument to " + getName() + " must be a constant UInt8", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        size_t index = index_col->getValue<UInt8>();

        const DataTypeTuple * tuple = checkAndGetDataType<DataTypeTuple>(&*arguments[0].type);
        if (!tuple)
            throw Exception("First argument for function " + getName() + " must be tuple.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (index == 0)
            throw Exception("Indices in tuples are 1-based.", ErrorCodes::ILLEGAL_INDEX);

        const DataTypes & elems = tuple->getElements();

        if (index > elems.size())
            throw Exception("Index for tuple element is out of range.", ErrorCodes::ILLEGAL_INDEX);

        out_return_type = elems[index - 1]->clone();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        const ColumnTuple * tuple_col = typeid_cast<const ColumnTuple *>(block.getByPosition(arguments[0]).column.get());
        auto const_tuple_col = checkAndGetColumnConst<ColumnTuple>(block.getByPosition(arguments[0]).column.get());
        auto index_col = checkAndGetColumnConst<ColumnUInt8>(block.getByPosition(arguments[1]).column.get());

        if (!tuple_col && !const_tuple_col)
            throw Exception("First argument for function " + getName() + " must be tuple.", ErrorCodes::ILLEGAL_COLUMN);

        if (!index_col)
            throw Exception("Second argument for function " + getName() + " must be UInt8 constant literal.", ErrorCodes::ILLEGAL_COLUMN);

        size_t index = index_col->getValue<UInt8>();
        if (index == 0)
            throw Exception("Indices in tuples is 1-based.", ErrorCodes::ILLEGAL_INDEX);

        if (tuple_col)
        {
            const Block & tuple_block = tuple_col->getData();

            if (index > tuple_block.columns())
                throw Exception("Index for tuple element is out of range.", ErrorCodes::ILLEGAL_INDEX);

            block.getByPosition(result).column = tuple_block.getByPosition(index - 1).column;
        }
        else
        {
            TupleBackend data = const_tuple_col->getValue<Tuple>();
            block.getByPosition(result).column = static_cast<const DataTypeTuple &>(*block.getByPosition(arguments[0]).type)
                .getElements()[index - 1]->createConstColumn(block.rows(), data[index - 1]);
        }
    }
};


class FunctionIgnore : public IFunction
{
public:
    static constexpr auto name = "ignore";
    static FunctionPtr create(const Context & context)
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        block.getByPosition(result).column = DataTypeUInt8().createConstColumn(block.rows(), UInt64(0));
    }
};


/** The `indexHint` function takes any number of any arguments and always returns one.
  *
  * This function has a special meaning (see ExpressionAnalyzer, PKCondition)
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
    static FunctionPtr create(const Context & context)
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
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        block.getByPosition(result).column = DataTypeUInt8().createConstColumn(block.rows(), UInt64(1));
    }
};


class FunctionIdentity : public IFunction
{
public:
    static constexpr auto name = "identity";
    static FunctionPtr create(const Context & context)
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
        return arguments.front()->clone();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        block.getByPosition(result).column = block.getByPosition(arguments.front()).column;
    }
};


class FunctionArrayJoin : public IFunction
{
public:
    static constexpr auto name = "arrayJoin";
    static FunctionPtr create(const Context & context)
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
    bool isDeterministicInScopeOfQuery() override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * arr = checkAndGetDataType<DataTypeArray>(&*arguments[0]);
        if (!arr)
            throw Exception("Argument for function " + getName() + " must be Array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arr->getNestedType()->clone();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        throw Exception("Function " + getName() + " must not be executed directly.", ErrorCodes::FUNCTION_IS_SPECIAL);
    }

    /// Because of function cannot be executed directly.
    bool isSuitableForConstantFolding() const override
    {
        return false;
    }
};


FunctionPtr FunctionReplicate::create(const Context & context)
{
    return std::make_shared<FunctionReplicate>();
}

DataTypePtr FunctionReplicate::getReturnTypeImpl(const DataTypes & arguments) const
{
    const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(&*arguments[1]);
    if (!array_type)
        throw Exception("Second argument for function " + getName() + " must be array.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<DataTypeArray>(arguments[0]->clone());
}

void FunctionReplicate::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
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
        = std::make_shared<ColumnArray>(first_column->replicate(array_column->getOffsets()), array_column->getOffsetsColumn());
}

/** Returns a string with nice Unicode-art bar with resolution of 1/8 part of symbol.
  */
class FunctionBar : public IFunction
{
public:
    static constexpr auto name = "bar";
    static FunctionPtr create(const Context & context)
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

        if (!arguments[0]->isNumeric() || !arguments[1]->isNumeric() || !arguments[2]->isNumeric()
            || (arguments.size() == 4 && !arguments[3]->isNumeric()))
            throw Exception("All arguments for function " + getName() + " must be numeric.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
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

        auto res_column = std::make_shared<ColumnString>();
        block.getByPosition(result).column = res_column;

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

        if (!column.isConst())
            throw Exception(
                which_argument + String(" argument for function ") + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);

        return applyVisitor(FieldVisitorConvertToNumber<T>(), column[0]);
    }

    template <typename T>
    static void fill(const PaddedPODArray<T> & src,
        ColumnString::Chars_t & dst_chars,
        ColumnString::Offsets_t & dst_offsets,
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
        UnicodeBar::render(width, &dst_chars[0]);
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
        const auto arg = arguments.front().get();
        if (!checkDataType<DataTypeUInt8>(arg) && !checkDataType<DataTypeUInt16>(arg)
            && !checkDataType<DataTypeUInt32>(arg)
            && !checkDataType<DataTypeUInt64>(arg)
            && !checkDataType<DataTypeInt8>(arg)
            && !checkDataType<DataTypeInt16>(arg)
            && !checkDataType<DataTypeInt32>(arg)
            && !checkDataType<DataTypeInt64>(arg)
            && !checkDataType<DataTypeFloat32>(arg)
            && !checkDataType<DataTypeFloat64>(arg))
            throw Exception{"Argument for function " + getName() + " must be numeric", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto in = block.getByPosition(arguments.front()).column.get();

        if (!execute<UInt8>(block, in, result) && !execute<UInt16>(block, in, result) && !execute<UInt32>(block, in, result)
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

            const auto out = std::make_shared<ColumnUInt8>(size);
            block.getByPosition(result).column = out;

            const auto & in_data = in->getData();
            auto & out_data = out->getData();

            for (const auto i : ext::range(0, size))
                out_data[i] = Impl::execute(in_data[i]);

            return true;
        }

        return false;
    }
};

struct IsFiniteImpl
{
    static constexpr auto name = "isFinite";
    template <typename T>
    static bool execute(const T t)
    {
        return std::isfinite(t);
    }
};

struct IsInfiniteImpl
{
    static constexpr auto name = "isInfinite";
    template <typename T>
    static bool execute(const T t)
    {
        return std::isinf(t);
    }
};

struct IsNaNImpl
{
    static constexpr auto name = "isNaN";
    template <typename T>
    static bool execute(const T t)
    {
        return std::isnan(t);
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
    static FunctionPtr create(const Context & context)
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        static const std::string version = getVersion();
        block.getByPosition(result).column = DataTypeString().createConstColumn(block.rows(), version);
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

    FunctionUptime(time_t uptime_) : uptime(uptime_)
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeUInt32>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        block.getByPosition(result).column = DataTypeUInt32().createConstColumn(block.rows(), uptime);
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
    static FunctionPtr create(const Context & context)
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        block.getByPosition(result).column = DataTypeString().createConstColumn(block.rows(), DateLUT::instance().getTimeZone());
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
    static FunctionPtr create(const Context & context)
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

    bool isDeterministicInScopeOfQuery() override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeAggregateFunction * type = checkAndGetDataType<DataTypeAggregateFunction>(&*arguments[0]);
        if (!type)
            throw Exception("Argument for function " + getName() + " must have type AggregateFunction - state of aggregate function.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return type->getReturnType()->clone();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
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

        auto deleter = [&agg_func](char * ptr) {
            agg_func.destroy(ptr);
            free(ptr);
        };
        std::unique_ptr<char, decltype(deleter)> place{reinterpret_cast<char *>(malloc(agg_func.sizeOfData())), deleter};

        agg_func.create(place.get()); /// Not much exception-safe. If an exception is thrown out, destroy will be called in vain.

        std::unique_ptr<Arena> arena = agg_func.allocatesMemoryInArena() ? std::make_unique<Arena>() : nullptr;

        ColumnPtr result_column_ptr = agg_func.getReturnType()->createColumn();
        block.getByPosition(result).column = result_column_ptr;
        IColumn & result_column = *result_column_ptr;
        result_column.reserve(column_with_states->size());

        const auto & states = column_with_states->getData();
        for (const auto & state_to_add : states)
        {
            /// Will pass empty arena if agg_func does not allocate memory in arena
            agg_func.merge(place.get(), state_to_add, arena.get());
            agg_func.insertResultInto(place.get(), result_column);
        }
    }
};


/** Calculate difference of consecutive values in block.
  * So, result of function depends on partition of data to blocks and on order of data in block.
  */
class FunctionRunningDifference : public IFunction
{
private:
    /// It is possible to track value from previous block, to calculate continuously across all blocks. Not implemented.

    template <typename Src, typename Dst>
    static void process(const PaddedPODArray<Src> & src, PaddedPODArray<Dst> & dst)
    {
        size_t size = src.size();
        dst.resize(size);

        if (size == 0)
            return;

        /// It is possible to SIMD optimize this loop. By no need for that in practice.

        dst[0] = 0;
        Src prev = src[0];
        for (size_t i = 1; i < size; ++i)
        {
            auto cur = src[i];
            dst[i] = static_cast<Dst>(cur) - prev;
            prev = cur;
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
    static constexpr auto name = "runningDifference";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionRunningDifference>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isDeterministicInScopeOfQuery() override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypePtr res;
        dispatchForSourceType(*arguments[0], [&](auto field_type_tag) {
            res = std::make_shared<DataTypeNumber<DstFieldType<decltype(field_type_tag)>>>();
        });

        return res;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        auto & src = block.getByPosition(arguments.at(0));
        auto & res = block.getByPosition(result);

        /// When column is constant, its difference is zero.
        if (src.column->isConst())
        {
            res.column = res.type->createConstColumn(block.rows(), res.type->getDefault());
            return;
        }

        res.column = res.type->createColumn();

        dispatchForSourceType(*src.type, [&](auto field_type_tag) {
            using SrcFieldType = decltype(field_type_tag);
            process(static_cast<const ColumnVector<SrcFieldType> &>(*src.column).getData(),
                static_cast<ColumnVector<DstFieldType<SrcFieldType>> &>(*res.column).getData());
        });
    }
};


/** Takes state of aggregate function. Returns result of aggregation (finalized state).
  */
class FunctionFinalizeAggregation : public IFunction
{
public:
    static constexpr auto name = "finalizeAggregation";
    static FunctionPtr create(const Context & context)
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
        const DataTypeAggregateFunction * type = checkAndGetDataType<DataTypeAggregateFunction>(&*arguments[0]);
        if (!type)
            throw Exception("Argument for function " + getName() + " must have type AggregateFunction - state of aggregate function.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return type->getReturnType()->clone();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        ColumnAggregateFunction * column_with_states
            = typeid_cast<ColumnAggregateFunction *>(&*block.getByPosition(arguments.at(0)).column);
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

    FunctionHasColumnInTable(const Context & global_context_) : global_context(global_context_)
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

    void getReturnTypeAndPrerequisitesImpl(
        const ColumnsWithTypeAndName & arguments, DataTypePtr & out_return_type, ExpressionActions::Actions & out_prerequisites) override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;

private:
    const Context & global_context;
};


void FunctionVisibleWidth::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    auto & src = block.getByPosition(arguments[0]);
    size_t size = block.rows();

    auto res_col = std::make_shared<ColumnUInt64>(size);
    auto & res_data = static_cast<ColumnUInt64 &>(*res_col).getData();
    block.getByPosition(result).column = res_col;

    /// For simplicity reasons, function is implemented by serializing into temporary buffer.

    String tmp;
    for (size_t i = 0; i < size; ++i)
    {
        {
            WriteBufferFromString out(tmp);
            src.type->serializeTextEscaped(*src.column, i, out);
        }

        res_data[i] = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(tmp.data()), tmp.size());
    }
}


void FunctionHasColumnInTable::getReturnTypeAndPrerequisitesImpl(
    const ColumnsWithTypeAndName & arguments, DataTypePtr & out_return_type, ExpressionActions::Actions & out_prerequisites)
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

    out_return_type = std::make_shared<DataTypeUInt8>();
}


void FunctionHasColumnInTable::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
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
        auto cluster = std::make_shared<Cluster>(global_context.getSettings(), host_names, !user_name.empty() ? user_name : "default", password);
        auto names_and_types_list = std::make_shared<NamesAndTypesList>(getStructureOfRemoteTable(*cluster, database_name, table_name, global_context));
        const auto & names = names_and_types_list->getNames();
        has_column = std::find(names.begin(), names.end(), column_name) != names.end();
    }

    block.getByPosition(result).column = DataTypeUInt8().createConstColumn(block.rows(), UInt64(has_column));
}


std::string FunctionVersion::getVersion() const
{
    std::ostringstream os;
    os << DBMS_VERSION_MAJOR << "." << DBMS_VERSION_MINOR << "." << ClickHouseRevision::get();
    return os.str();
}


void registerFunctionsMiscellaneous(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCurrentDatabase>();
    factory.registerFunction<FunctionHostName>();
    factory.registerFunction<FunctionVisibleWidth>();
    factory.registerFunction<FunctionToTypeName>();
    factory.registerFunction<FunctionToColumnTypeName>();
    factory.registerFunction<FunctionDefaultValueOfArgumentType>();
    factory.registerFunction<FunctionBlockSize>();
    factory.registerFunction<FunctionBlockNumber>();
    factory.registerFunction<FunctionRowNumberInBlock>();
    factory.registerFunction<FunctionRowNumberInAllBlocks>();
    factory.registerFunction<FunctionSleep>();
    factory.registerFunction<FunctionMaterialize>();
    factory.registerFunction<FunctionIgnore>();
    factory.registerFunction<FunctionIndexHint>();
    factory.registerFunction<FunctionIdentity>();
    factory.registerFunction<FunctionArrayJoin>();
    factory.registerFunction<FunctionReplicate>();
    factory.registerFunction<FunctionBar>();
    factory.registerFunction<FunctionHasColumnInTable>();

    factory.registerFunction<FunctionTuple>();
    factory.registerFunction<FunctionTupleElement>();
    factory.registerFunction<FunctionIn<false, false>>();
    factory.registerFunction<FunctionIn<false, true>>();
    factory.registerFunction<FunctionIn<true, false>>();
    factory.registerFunction<FunctionIn<true, true>>();

    factory.registerFunction<FunctionIsFinite>();
    factory.registerFunction<FunctionIsInfinite>();
    factory.registerFunction<FunctionIsNaN>();

    factory.registerFunction<FunctionVersion>();
    factory.registerFunction<FunctionUptime>();
    factory.registerFunction<FunctionTimeZone>();

    factory.registerFunction<FunctionRunningAccumulate>();
    factory.registerFunction<FunctionRunningDifference>();
    factory.registerFunction<FunctionFinalizeAggregation>();
}
}
