#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/queryNormalization.h>
#include <base/find_symbols.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/SipHash.h>


/** The function returns 64bit hash value that is identical for similar queries.
  * See also 'normalizeQuery'. This function is only slightly more efficient.
  */

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <bool keep_names>
struct Impl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        PaddedPODArray<UInt64> & res_data)
    {
        size_t size = offsets.size();
        res_data.resize(size);

        ColumnString::Offset prev_src_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset curr_src_offset = offsets[i];
            res_data[i] = normalizedQueryHash<keep_names>(
                reinterpret_cast<const char *>(&data[prev_src_offset]), reinterpret_cast<const char *>(&data[curr_src_offset - 1]));
            prev_src_offset = offsets[i];
        }
    }
};

template <bool keep_names>
class FunctionNormalizedQueryHash : public IFunction
{
public:
    static constexpr auto name = keep_names ? "normalizedQueryHashKeepNames" : "normalizedQueryHash";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionNormalizedQueryHash>();
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
        if (!isString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr column = arguments[0].column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnUInt64::create();
            typename ColumnUInt64::Container & vec_res = col_res->getData();
            vec_res.resize(col->size());
            Impl<keep_names>::vector(col->getChars(), col->getOffsets(), vec_res);
            return col_res;
        }
        else
            throw Exception("Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

}

void registerFunctionNormalizedQueryHash(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNormalizedQueryHash<true>>();
    factory.registerFunction<FunctionNormalizedQueryHash<false>>();
}

}

