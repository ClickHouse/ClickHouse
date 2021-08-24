#if !defined(ARCADIA_BUILD)
#    include "config_functions.h"
#endif

#if USE_H3

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>

#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

using namespace GatherUtils;

class FunctionStringToH3 : public IFunction
{
public:
    static constexpr auto name = "stringToH3";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionStringToH3>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isStringOrFixedString())
            throw Exception(
                "Illegal type " + arg->getName() + " of argument " + std::to_string(1) + " of function " + getName() + ". Must be String or FixedString",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_hindex = arguments[0].column.get();

        auto dst = ColumnVector<UInt64>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        if (const auto * h3index = checkAndGetColumn<ColumnString>(col_hindex))
            execute<StringSource>(StringSource(*h3index), dst_data);
        else if (const auto * h3index_fixed = checkAndGetColumn<ColumnFixedString>(col_hindex))
            execute<FixedStringSource>(FixedStringSource(*h3index_fixed), dst_data);
        else if (const ColumnConst * h3index_const = checkAndGetColumnConst<ColumnString>(col_hindex))
            execute<ConstSource<StringSource>>(ConstSource<StringSource>(*h3index_const), dst_data);
        else if (const ColumnConst * h3index_const_fixed = checkAndGetColumnConst<ColumnFixedString>(col_hindex))
            execute<ConstSource<FixedStringSource>>(ConstSource<FixedStringSource>(*h3index_const_fixed), dst_data);
        else
            throw Exception("Illegal column as argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        return dst;
    }

private:
    template <typename H3IndexSource>
    static void execute(H3IndexSource h3index_source, PaddedPODArray<UInt64> & res_data)
    {
        size_t row_num = 0;

        while (!h3index_source.isEnd())
        {
            auto h3index = h3index_source.getWhole();

            // covert to std::string and get the c_str to have the delimiting \0 at the end.
            auto h3index_str = StringRef(h3index.data, h3index.size).toString();
            res_data[row_num] = stringToH3(h3index_str.c_str());

            if (res_data[row_num] == 0)
            {
                throw Exception("Invalid H3 index: " + h3index_str, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            h3index_source.next();
            ++row_num;
        }
    }
};

}

void registerFunctionStringToH3(FunctionFactory & factory)
{
    factory.registerFunction<FunctionStringToH3>();
}

}

#endif
