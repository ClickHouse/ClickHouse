#include <Columns/IColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/Kusto/KqlFunctionBase.h>

namespace DB
{

bool KqlFunctionBase::check_condition(const ColumnWithTypeAndName & condition, ContextPtr context, size_t input_rows_count)
{
    ColumnsWithTypeAndName if_columns(
    {
        condition,
        {DataTypeUInt8().createColumnConst(1, toField(UInt8(1))), std::make_shared<DataTypeUInt8>(), ""},
        {DataTypeUInt8().createColumnConst(1, toField(UInt8(2))), std::make_shared<DataTypeUInt8>(), ""}
    });
    auto if_res = FunctionFactory::instance().get("if", context)->build(if_columns)->execute(if_columns, std::make_shared<DataTypeUInt8>(), input_rows_count, /* dry_run = */ false);
    auto result = if_res->getUInt(0);
    return (result == 1);
}

}
