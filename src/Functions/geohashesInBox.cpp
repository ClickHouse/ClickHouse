#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GeoHash.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>

#include <memory>
#include <string>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{

class FunctionGeohashesInBox : public IFunction
{
public:
    static constexpr auto name = "geohashesInBox";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeohashesInBox>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 5; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        validateArgumentType(*this, arguments, 0, isFloat, "float");
        validateArgumentType(*this, arguments, 1, isFloat, "float");
        validateArgumentType(*this, arguments, 2, isFloat, "float");
        validateArgumentType(*this, arguments, 3, isFloat, "float");
        validateArgumentType(*this, arguments, 4, isUInt8, "integer");

        if (!(arguments[0]->equals(*arguments[1]) &&
              arguments[0]->equals(*arguments[2]) &&
              arguments[0]->equals(*arguments[3])))
        {
            throw Exception("Illegal type of argument of " + getName() +
                            " all coordinate arguments must have the same type, instead they are:" +
                            arguments[0]->getName() + ", " +
                            arguments[1]->getName() + ", " +
                            arguments[2]->getName() + ", " +
                            arguments[3]->getName() + ".",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    template <typename LonAndLatType, typename PrecisionType>
    void execute(
        const IColumn * lon_min_column,
        const IColumn * lat_min_column,
        const IColumn * lon_max_column,
        const IColumn * lat_max_column,
        const IColumn * precision_column,
        ColumnPtr & result,
        size_t input_rows_count) const
    {
        static constexpr size_t max_array_size = 10'000'000;

        const auto * lon_min_const = typeid_cast<const ColumnConst *>(lon_min_column);
        const auto * lat_min_const = typeid_cast<const ColumnConst *>(lat_min_column);
        const auto * lon_max_const = typeid_cast<const ColumnConst *>(lon_max_column);
        const auto * lat_max_const = typeid_cast<const ColumnConst *>(lat_max_column);
        const auto * precision_const = typeid_cast<const ColumnConst *>(precision_column);

        if (lon_min_const)
            lon_min_column = &lon_min_const->getDataColumn();
        if (lat_min_const)
            lat_min_column = &lat_min_const->getDataColumn();
        if (lon_max_const)
            lon_max_column = &lon_max_const->getDataColumn();
        if (lat_max_const)
            lat_max_column = &lat_max_const->getDataColumn();
        if (precision_const)
            precision_column = &precision_const->getDataColumn();

        const auto * lon_min = checkAndGetColumn<ColumnVector<LonAndLatType>>(lon_min_column);
        const auto * lat_min = checkAndGetColumn<ColumnVector<LonAndLatType>>(lat_min_column);
        const auto * lon_max = checkAndGetColumn<ColumnVector<LonAndLatType>>(lon_max_column);
        const auto * lat_max = checkAndGetColumn<ColumnVector<LonAndLatType>>(lat_max_column);
        const auto * precision = checkAndGetColumn<ColumnVector<PrecisionType>>(precision_column);

        if (!lon_min || !lat_min || !lon_max || !lat_max || !precision)
        {
            throw Exception("Unsupported argument types for function " + getName() + " : " +
                            lon_min_column->getName() + ", " +
                            lat_min_column->getName() + ", " +
                            lon_max_column->getName() + ", " +
                            lat_max_column->getName() + ".",
                            ErrorCodes::LOGICAL_ERROR);
        }

        auto col_res = ColumnArray::create(ColumnString::create());
        ColumnString & res_strings = typeid_cast<ColumnString &>(col_res->getData());
        ColumnArray::Offsets & res_offsets = col_res->getOffsets();
        ColumnString::Chars & res_strings_chars = res_strings.getChars();
        ColumnString::Offsets & res_strings_offsets = res_strings.getOffsets();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const Float64 lon_min_value = lon_min->getElement(lon_min_const ? 0 : row);
            const Float64 lat_min_value = lat_min->getElement(lat_min_const ? 0 : row);
            const Float64 lon_max_value = lon_max->getElement(lon_max_const ? 0 : row);
            const Float64 lat_max_value = lat_max->getElement(lat_max_const ? 0 : row);
            const PrecisionType precision_value = precision->getElement(precision_const ? 0 : row);

            const auto prepared_args = geohashesInBoxPrepare(
                lon_min_value, lat_min_value, lon_max_value, lat_max_value,
                precision_value);

            if (prepared_args.items_count > max_array_size)
            {
                throw Exception(getName() + " would produce " + std::to_string(prepared_args.items_count) +
                                " array elements, which is bigger than the allowed maximum of " + std::to_string(max_array_size),
                                ErrorCodes::TOO_LARGE_ARRAY_SIZE);
            }

            res_strings_offsets.reserve(res_strings_offsets.size() + prepared_args.items_count);
            res_strings_chars.resize(res_strings_chars.size() + prepared_args.items_count * (prepared_args.precision + 1));
            const auto starting_offset = res_strings_offsets.empty() ? 0 : res_strings_offsets.back();
            char * out = reinterpret_cast<char *>(res_strings_chars.data() + starting_offset);

            // Actually write geohashes into preallocated buffer.
            geohashesInBox(prepared_args, out);

            for (UInt64 i = 1; i <= prepared_args.items_count ; ++i)
                res_strings_offsets.push_back(starting_offset + (prepared_args.precision + 1) * i);
            res_offsets.push_back(res_offsets.back() + prepared_args.items_count);
        }

        if (!res_strings_offsets.empty() && res_strings_offsets.back() != res_strings_chars.size())
        {
            throw Exception("String column size mismatch (internal logical error)", ErrorCodes::LOGICAL_ERROR);
        }

        if (!res_offsets.empty() && res_offsets.back() != res_strings.size())
        {
            throw Exception("Array column size mismatch (internal logical error)" +
                            std::to_string(res_offsets.back()) + " != " + std::to_string(res_strings.size()),
                            ErrorCodes::LOGICAL_ERROR);
        }

        result = std::move(col_res);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * lon_min = arguments[0].column.get();
        const IColumn * lat_min = arguments[1].column.get();
        const IColumn * lon_max = arguments[2].column.get();
        const IColumn * lat_max = arguments[3].column.get();
        const IColumn * precision = arguments[4].column.get();
        ColumnPtr res;

        if (checkColumn<ColumnVector<Float32>>(lon_min))
            execute<Float32, UInt8>(lon_min, lat_min, lon_max, lat_max, precision, res, input_rows_count);
        else
            execute<Float64, UInt8>(lon_min, lat_min, lon_max, lat_max, precision, res, input_rows_count);

        return res;
    }
};

}

void registerFunctionGeohashesInBox(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGeohashesInBox>();
}

}
