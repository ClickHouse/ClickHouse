#include <Functions/FunctionFactory.h>
#include <Functions/GeoHash.h>
#include <Functions/FunctionHelpers.h>

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <string>

#define GEOHASH_MAX_TEXT_LENGTH 16


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

// geohashEncode(lon float32/64, lat float32/64, length UInt8) => string
class FunctionGeohashEncode : public IFunction
{
public:
    static constexpr auto name = "geohashEncode";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeohashEncode>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"longitude", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isFloat), nullptr, "Float*"},
            {"latitude", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isFloat), nullptr, "Float*"}
        };
        FunctionArgumentDescriptors optional_args{
            {"precision", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "(U)Int*"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * longitude = arguments[0].column.get();
        const IColumn * latitude = arguments[1].column.get();

        ColumnPtr precision;
        if (arguments.size() < 3)
            precision = DataTypeUInt8().createColumnConst(longitude->size(), GEOHASH_MAX_TEXT_LENGTH);
        else
            precision = arguments[2].column;

        ColumnPtr res_column;
        vector(longitude, latitude, precision.get(), res_column, input_rows_count);
        return res_column;
    }

private:
    void vector(const IColumn * lon_column, const IColumn * lat_column, const IColumn * precision_column, ColumnPtr & result, size_t input_rows_count) const
    {
        auto col_str = ColumnString::create();
        ColumnString::Chars & out_vec = col_str->getChars();
        ColumnString::Offsets & out_offsets = col_str->getOffsets();

        out_offsets.resize(input_rows_count);
        out_vec.resize(input_rows_count * (GEOHASH_MAX_TEXT_LENGTH + 1));

        char * begin = reinterpret_cast<char *>(out_vec.data());
        char * pos = begin;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const Float64 longitude_value = lon_column->getFloat64(i);
            const Float64 latitude_value = lat_column->getFloat64(i);
            const UInt64 precision_value = std::min<UInt64>(precision_column->get64(i), GEOHASH_MAX_TEXT_LENGTH);

            const size_t encoded_size = geohashEncode(longitude_value, latitude_value, precision_value, pos);

            pos += encoded_size;
            *pos = '\0';
            out_offsets[i] = ++pos - begin;
        }
        out_vec.resize(pos - begin);

        if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column size mismatch (internal logical error)");

        result = std::move(col_str);
    }
};

}

REGISTER_FUNCTION(GeohashEncode)
{
    factory.registerFunction<FunctionGeohashEncode>();
}

}
