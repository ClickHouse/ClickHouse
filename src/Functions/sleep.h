#pragma once
#include <unistd.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <base/sleep.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>

namespace ProfileEvents
{
extern const Event SleepFunctionCalls;
extern const Event SleepFunctionMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_SLOW;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

/** sleep(seconds) - the specified number of seconds sleeps each columns.
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
    static FunctionPtr create(ContextPtr)
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

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        WhichDataType which(arguments[0]);

        if (!which.isFloat()
            && !which.isNativeUInt())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName() + ", expected Float64",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        const IColumn * col = arguments[0].column.get();

        if (!isColumnConst(*col))
            throw Exception("The argument of function " + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);

        Float64 seconds = applyVisitor(FieldVisitorConvertToNumber<Float64>(), assert_cast<const ColumnConst &>(*col).getField());

        if (seconds < 0 || !std::isfinite(seconds))
            throw Exception("Cannot sleep infinite or negative amount of time (not implemented)", ErrorCodes::BAD_ARGUMENTS);

        size_t size = col->size();

        /// We do not sleep if the columns is empty.
        if (size > 0)
        {
            /// When sleeping, the query cannot be cancelled. For ability to cancel query, we limit sleep time.
            if (seconds > 3.0)   /// The choice is arbitrary
                throw Exception("The maximum sleep time is 3 seconds. Requested: " + toString(seconds), ErrorCodes::TOO_SLOW);

            UInt64 count = (variant == FunctionSleepVariant::PerBlock ? 1 : size);
            UInt64 microseconds = seconds * count * 1e6;
            sleepForMicroseconds(microseconds);
            ProfileEvents::increment(ProfileEvents::SleepFunctionCalls, count);
            ProfileEvents::increment(ProfileEvents::SleepFunctionMicroseconds, microseconds);
        }

        /// convertToFullColumn needed, because otherwise (constant expression case) function will not get called on each columns.
        return result_type->createColumnConst(size, 0u)->convertToFullColumnIfConst();
    }
};

}
