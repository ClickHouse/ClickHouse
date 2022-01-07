#pragma once

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/GatherUtils/Sinks.h>
#include <Functions/GatherUtils/Slices.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <IO/WriteHelpers.h>


namespace DB
{

using namespace GatherUtils;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <TrimMode mode>
class FunctionsTrim : public IFunction
{
public:
    static constexpr auto name = mode == TrimMode::Left
        ? "trimLeft"
        : (mode == TrimMode::Right
               ? "trimRight"
               : "trim");

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionsTrim>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of first argument of function {}", arguments[0]->getName(), getName());

        if (!isStringOrFixedString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of second argument of function {}", arguments[0]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    template <typename Source>
    ColumnPtr executeForSource(Source && source, const ColumnPtr & needle, size_t input_rows_count) const
    {
        auto col_res = ColumnString::create();
        StringSink sink(col_res, input_rows_count);

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(needle.get()))
            trim<mode>(source, StringSource(*col), sink);
        else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(needle.get()))
            trim<mode>(source, FixedStringSource(*col_fixed), sink);
        else if (const ColumnConst * col_const = checkAndGetColumnConst<ColumnString>(needle.get()))
            trim<mode>(source, ConstSource<StringSource>(*col_const), sink);
        else if (const ColumnConst * col_const_fixed = checkAndGetColumnConst<ColumnFixedString>(needle.get()))
            trim<mode>(source, ConstSource<FixedStringSource>(*col_const_fixed), sink);
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());

        return col_res;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr column_src = arguments[0].column;
        ColumnPtr column_needle = arguments[1].column;

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_src.get()))
            return executeForSource(StringSource(*col), column_needle, input_rows_count);
        else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column_src.get()))
            return executeForSource(FixedStringSource(*col_fixed), column_needle, input_rows_count);
        else if (const ColumnConst * col_const = checkAndGetColumnConst<ColumnString>(column_src.get()))
            return executeForSource(ConstSource<StringSource>(*col_const), column_needle, input_rows_count);
        else if (const ColumnConst * col_const_fixed = checkAndGetColumnConst<ColumnFixedString>(column_src.get()))
            return executeForSource(ConstSource<FixedStringSource>(*col_const_fixed), column_needle, input_rows_count);
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());
    }
};

}
