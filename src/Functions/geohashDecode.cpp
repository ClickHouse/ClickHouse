#include <Functions/FunctionFactory.h>
#include <Functions/GeoHash.h>
#include <Functions/FunctionHelpers.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

#include <string>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


// geohashDecode(string) => (lon float64, lat float64)
class FunctionGeohashDecode : public IFunction
{
public:
    static constexpr auto name = "geohashDecode";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionGeohashDecode>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        validateArgumentType(*this, arguments, 0, isStringOrFixedString, "string or fixed string");

        return std::make_shared<DataTypeTuple>(
                DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()},
                Strings{"longitude", "latitude"});
    }

    template <typename ColumnTypeEncoded>
    bool tryExecute(const IColumn * encoded_column, ColumnPtr & result_column)
    {
        const auto * encoded = checkAndGetColumn<ColumnTypeEncoded>(encoded_column);
        if (!encoded)
            return false;

        const size_t count = encoded->size();

        auto latitude = ColumnFloat64::create(count);
        auto longitude = ColumnFloat64::create(count);

        ColumnFloat64::Container & lon_data = longitude->getData();
        ColumnFloat64::Container & lat_data = latitude->getData();

        for (size_t i = 0; i < count; ++i)
        {
            StringRef encoded_string = encoded->getDataAt(i);
            geohashDecode(encoded_string.data, encoded_string.size, &lon_data[i], &lat_data[i]);
        }

        MutableColumns result;
        result.emplace_back(std::move(longitude));
        result.emplace_back(std::move(latitude));
        result_column = ColumnTuple::create(std::move(result));

        return true;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const IColumn * encoded = block.getByPosition(arguments[0]).column.get();
        ColumnPtr & res_column = block.getByPosition(result).column;

        if (tryExecute<ColumnString>(encoded, res_column) ||
            tryExecute<ColumnFixedString>(encoded, res_column))
            return;

        throw Exception("Unsupported argument type:" + block.getByPosition(arguments[0]).column->getName()
                        + " of argument of function " + getName(),
                        ErrorCodes::ILLEGAL_COLUMN);
    }
};


void registerFunctionGeohashDecode(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGeohashDecode>();
}

}
