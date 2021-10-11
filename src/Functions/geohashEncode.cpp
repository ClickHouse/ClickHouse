#include <Functions/FunctionFactory.h>
#include <Functions/GeoHash.h>
#include <Functions/FunctionHelpers.h>

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>

#include <string>

#define GEOHASH_MAX_TEXT_LENGTH 16


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
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
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }
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
            throw Exception("Too many arguments for function " + getName() +
                            " expected at most 3",
                            ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);
        }

        return std::make_shared<DataTypeString>();
    }

    template <typename LonType, typename LatType>
    bool tryExecute(const IColumn * lon_column, const IColumn * lat_column, UInt64 precision_value, ColumnPtr & result) const
    {
        const ColumnVector<LonType> * longitude = checkAndGetColumn<ColumnVector<LonType>>(lon_column);
        const ColumnVector<LatType> * latitude = checkAndGetColumn<ColumnVector<LatType>>(lat_column);
        if (!latitude || !longitude)
            return false;

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
            const Float64 longitude_value = longitude->getElement(i);
            const Float64 latitude_value = latitude->getElement(i);

            const size_t encoded_size = geohashEncode(longitude_value, latitude_value, precision_value, pos);

            pos += encoded_size;
            *pos = '\0';
            out_offsets[i] = ++pos - begin;
        }
        out_vec.resize(pos - begin);

        if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
            throw Exception("Column size mismatch (internal logical error)", ErrorCodes::LOGICAL_ERROR);

        result = std::move(col_str);

        return true;

    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const IColumn * longitude = arguments[0].column.get();
        const IColumn * latitude = arguments[1].column.get();

        const UInt64 precision_value = std::min<UInt64>(GEOHASH_MAX_TEXT_LENGTH,
                arguments.size() == 3 ? arguments[2].column->get64(0) : GEOHASH_MAX_TEXT_LENGTH);

        ColumnPtr res_column;

        if (tryExecute<Float32, Float32>(longitude, latitude, precision_value, res_column) ||
            tryExecute<Float64, Float32>(longitude, latitude, precision_value, res_column) ||
            tryExecute<Float32, Float64>(longitude, latitude, precision_value, res_column) ||
            tryExecute<Float64, Float64>(longitude, latitude, precision_value, res_column))
            return res_column;

        std::string arguments_description;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (i != 0)
                arguments_description += ", ";
            arguments_description += arguments[i].column->getName();
        }

        throw Exception("Unsupported argument types: " + arguments_description +
                        + " for function " + getName(),
                        ErrorCodes::ILLEGAL_COLUMN);
    }
};

}

void registerFunctionGeohashEncode(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGeohashEncode>();
}

}
