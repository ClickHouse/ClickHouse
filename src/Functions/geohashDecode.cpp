#include <Functions/FunctionFactory.h>
#include <Functions/GeoHash.h>
#include <Functions/FunctionHelpers.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

#include <string>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

// geohashDecode(string) => (lon float64, lat float64)
class FunctionGeohashDecode : public IFunction
{
public:
    static constexpr auto name = "geohashDecode";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeohashDecode>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"encoded", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}
        };
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeTuple>(
                DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()},
                Strings{"longitude", "latitude"});
    }

    template <typename ColumnTypeEncoded>
    bool tryExecute(const IColumn * encoded_column, ColumnPtr & result_column, size_t input_rows_count) const
    {
        const auto * encoded = checkAndGetColumn<ColumnTypeEncoded>(encoded_column);
        if (!encoded)
            return false;

        auto latitude = ColumnFloat64::create(input_rows_count);
        auto longitude = ColumnFloat64::create(input_rows_count);

        ColumnFloat64::Container & lon_data = longitude->getData();
        ColumnFloat64::Container & lat_data = latitude->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view encoded_string = encoded->getDataAt(i).toView();
            geohashDecode(encoded_string.data(), encoded_string.size(), &lon_data[i], &lat_data[i]);
        }

        MutableColumns result;
        result.emplace_back(std::move(longitude));
        result.emplace_back(std::move(latitude));
        result_column = ColumnTuple::create(std::move(result));

        return true;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * encoded = arguments[0].column.get();
        ColumnPtr res_column;

        if (tryExecute<ColumnString>(encoded, res_column, input_rows_count) ||
            tryExecute<ColumnFixedString>(encoded, res_column, input_rows_count))
            return res_column;

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unsupported argument type:{} of argument of function {}",
                        arguments[0].column->getName(), getName());
    }
};

}

REGISTER_FUNCTION(GeohashDecode)
{
    factory.registerFunction<FunctionGeohashDecode>();
}

}
