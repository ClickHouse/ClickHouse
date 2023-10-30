#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include "Functions/array/FunctionArrayMapped.h"

namespace DB
{

class KqlFunctionBase : public IFunction
{
public:
    static bool check_condition (const ColumnWithTypeAndName & condition, ContextPtr context, size_t input_rows_count)
    {
        ColumnsWithTypeAndName if_columns(
        {
            condition,
            {DataTypeUInt8().createColumnConst(1, toField(UInt8(1))), std::make_shared<DataTypeUInt8>(), ""},
            {DataTypeUInt8().createColumnConst(1, toField(UInt8(2))), std::make_shared<DataTypeUInt8>(), ""}
        });
        auto if_res = FunctionFactory::instance().get("if", context)->build(if_columns)->execute(if_columns, std::make_shared<DataTypeUInt8>(), input_rows_count);
        auto result = if_res->getUInt(0);
        return (result == 1);
    }
};

}
