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
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        validateArgumentType(*this, arguments, 0, isFloat, "float");
        validateArgumentType(*this, arguments, 1, isFloat, "float");
        if (arguments.size() == 3)
        {
            validateArgumentType(*this, arguments, 2, isInteger, "integer");
        }
        if (arguments.size() > 3)
        {
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION, "Too many arguments for function {} expected at most 3",
                            getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const IColumn * longitude = arguments[0].column.get();
        const IColumn * latitude = arguments[1].column.get();

        ColumnPtr precision;
        if (arguments.size() < 3)
            precision = DataTypeUInt8().createColumnConst(longitude->size(), GEOHASH_MAX_TEXT_LENGTH);
        else
            precision = arguments[2].column;

        ColumnPtr res_column;
        vector(longitude, latitude, precision.get(), res_column);
        return res_column;
    }

private:
    void vector(const IColumn * lon_column, const IColumn * lat_column, const IColumn * precision_column, ColumnPtr & result) const
    {
        auto col_str = ColumnString::create();
        ColumnString::Chars & out_vec = col_str->getChars();
        ColumnString::Offsets & out_offsets = col_str->getOffsets();

        const size_t size = lat_column->size();

        out_offsets.resize(size);
        out_vec.resize(size * (GEOHASH_MAX_TEXT_LENGTH + 1));

        char * begin = reinterpret_cast<char *>(out_vec.data());
        char * pos = begin;

        for (size_t i = 0; i < size; ++i)
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
