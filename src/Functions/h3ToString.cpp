#include "config_functions.h"

#if USE_H3

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>

#include <h3api.h>

#define H3_INDEX_STRING_LENGTH 17 // includes \0 terminator

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class FunctionH3ToString : public IFunction
{
public:
    static constexpr auto name = "h3ToString";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3ToString>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arg->getName(), 1, getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_hindex = arguments[0].column.get();

        auto col_res = ColumnString::create();
        auto & vec_res = col_res->getChars();
        auto & vec_offsets = col_res->getOffsets();

        vec_offsets.resize(input_rows_count);
        vec_res.resize_fill(input_rows_count * H3_INDEX_STRING_LENGTH, '\0');

        char * begin = reinterpret_cast<char *>(vec_res.data());
        char * pos = begin;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const UInt64 hindex = col_hindex->getUInt(i);

            if (!isValidCell(hindex))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid H3 index: {}", hindex);

            h3ToString(hindex, pos, H3_INDEX_STRING_LENGTH);

            // move to end of the index
            while (*pos != '\0')
                pos++;

            vec_offsets[i] = ++pos - begin;
        }
        vec_res.resize(pos - begin);
        return col_res;
    }
};

}

void registerFunctionH3ToString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionH3ToString>();
}

}

#endif
