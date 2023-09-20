#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>
#include "Disks/DiskType.h"
#include "Interpreters/Context_fwd.h"
#include <Core/Field.h>
#include <Poco/Net/NameValueCollection.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/** Get the value of parameter in http headers.
  * If there no such parameter or the method of request is not
  * http, the function will return empty string.
  */
class FunctionGetHttpHeader : public IFunction, WithContext
{
private:

public:
    explicit FunctionGetHttpHeader(ContextPtr context_): WithContext(context_) {}

    static constexpr auto name = "getHttpHeader";

    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionGetHttpHeader>(context_);
    }


    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }


    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The argument of function {} must have String type", getName());
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & client_info = getContext()->getClientInfo();
        const auto & method = client_info.http_method;

        const auto & headers = DB::CurrentThread::getQueryContext()->getClientInfo().headers;

        const IColumn * arg_column = arguments[0].column.get();
        const ColumnString * arg_string = checkAndGetColumnConstData<ColumnString>(arg_column);

        if (!arg_string)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The argument of function {} must be constant String", getName());

        if (method != ClientInfo::HTTPMethod::GET && method != ClientInfo::HTTPMethod::POST)
            return result_type->createColumnConst(input_rows_count, "");

        if (!headers.has(arg_string->getDataAt(0).toString()))
            return result_type->createColumnConst(input_rows_count, "");

        return result_type->createColumnConst(input_rows_count, headers[arg_string->getDataAt(0).toString()]);
    }
};

}

REGISTER_FUNCTION(GetHttpHeader)
{
    factory.registerFunction<FunctionGetHttpHeader>();
}

}
