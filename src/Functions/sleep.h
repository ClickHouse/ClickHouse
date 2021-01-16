#include <unistd.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/FieldVisitors.h>
#include <Common/assert_cast.h>
#include <common/sleep.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_SLOW;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

/** sleep(seconds) - the specified number of seconds sleeps each block.
  */

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
        WhichDataType which(arguments[0]);

        if (!which.isFloat()
            && !which.isNativeUInt())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName() + ", expected Float64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        const IColumn * col = block.getByPosition(arguments[0]).column.get();

        if (!isColumnConst(*col))
            throw Exception("The argument of function " + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);

        Float64 seconds = applyVisitor(FieldVisitorConvertToNumber<Float64>(), assert_cast<const ColumnConst &>(*col).getField());

        if (seconds < 0)
            throw Exception("Cannot sleep negative amount of time (not implemented)", ErrorCodes::BAD_ARGUMENTS);

        size_t size = col->size();

        /// We do not sleep if the block is empty.
        if (size > 0)
        {
            /// When sleeping, the query cannot be cancelled. For ability to cancel query, we limit sleep time.
            if (seconds > 3.0)   /// The choice is arbitrary
                throw Exception("The maximum sleep time is 3 seconds. Requested: " + toString(seconds), ErrorCodes::TOO_SLOW);

            UInt64 microseconds = seconds * (variant == FunctionSleepVariant::PerBlock ? 1 : size) * 1e6;
            sleepForMicroseconds(microseconds);
        }

        /// convertToFullColumn needed, because otherwise (constant expression case) function will not get called on each block.
        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(size, 0u)->convertToFullColumnIfConst();
    }
};

}
