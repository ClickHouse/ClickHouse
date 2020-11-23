#include <Functions/FunctionFactory.h>
#include <Functions/castTypeToEither.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeUUID.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>

#include <Common/typeid_cast.h>
#include <Common/memcpySmall.h>

#include <common/unaligned.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
template <typename ToDataType, typename Name, bool support_between_float_integer>
class FunctionReinterpretAs : public IFunction
{
    template <typename F>
    static bool castType(const IDataType * type, F && f)
    {
        return castTypeToEither<DataTypeUInt32, DataTypeInt32, DataTypeUInt64, DataTypeInt64, DataTypeFloat32, DataTypeFloat64>(
            type, std::forward<F>(f));
    }

    template <typename From, typename To>
    static void reinterpretImpl(const PaddedPODArray<From> & from, PaddedPODArray<To> & to)
    {
        size_t size = from.size();
        to.resize(size);
        for (size_t i = 0; i < size; ++i)
        {
            to[i] = unalignedLoad<To>(&(from.data()[i]));
        }
    }

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionReinterpretAs>(); }

    using ToFieldType = typename ToDataType::FieldType;
    using ColumnType = typename ToDataType::ColumnType;

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const IDataType & type = *arguments[0];
        if constexpr (support_between_float_integer)
        {
            if (!isStringOrFixedString(type) && !isNumber(type))
                throw Exception(
                    "Cannot reinterpret " + type.getName() + " as " + ToDataType().getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (isNumber(type))
            {
                if (type.getSizeOfValueInMemory() != ToDataType{}.getSizeOfValueInMemory())
                    throw Exception(
                        "Cannot reinterpret " + type.getName() + " as " + ToDataType().getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }
        else
        {
            if (!isStringOrFixedString(type))
                throw Exception(
                    "Cannot reinterpret " + type.getName() + " as " + ToDataType().getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        return std::make_shared<ToDataType>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        if (const ColumnString * col_from = typeid_cast<const ColumnString *>(arguments[0].column.get()))
        {
            auto col_res = ColumnType::create();

            const ColumnString::Chars & data_from = col_from->getChars();
            const ColumnString::Offsets & offsets_from = col_from->getOffsets();
            size_t size = offsets_from.size();
            typename ColumnType::Container & vec_res = col_res->getData();
            vec_res.resize(size);

            size_t offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                ToFieldType value{};
                memcpy(&value, &data_from[offset], std::min(static_cast<UInt64>(sizeof(ToFieldType)), offsets_from[i] - offset - 1));
                vec_res[i] = value;
                offset = offsets_from[i];
            }

            return col_res;
        }
        else if (const ColumnFixedString * col_from_fixed = typeid_cast<const ColumnFixedString *>(arguments[0].column.get()))
        {
            auto col_res = ColumnVector<ToFieldType>::create();

            const ColumnString::Chars & data_from = col_from_fixed->getChars();
            size_t step = col_from_fixed->getN();
            size_t size = data_from.size() / step;
            typename ColumnVector<ToFieldType>::Container & vec_res = col_res->getData();
            vec_res.resize(size);

            size_t offset = 0;
            size_t copy_size = std::min(step, sizeof(ToFieldType));
            for (size_t i = 0; i < size; ++i)
            {
                ToFieldType value{};
                memcpy(&value, &data_from[offset], copy_size);
                vec_res[i] = value;
                offset += step;
            }

            return col_res;
        }
        else if constexpr (support_between_float_integer)
        {
            ColumnPtr res;
            if (castType(arguments[0].type.get(), [&](const auto & type)
                {
                    using DataType = std::decay_t<decltype(type)>;
                    using T = typename DataType::FieldType;

                    const ColumnVector<T> * col = checkAndGetColumn<ColumnVector<T>>(arguments[0].column.get());
                    auto col_res = ColumnType::create();
                    reinterpretImpl(col->getData(), col_res->getData());
                    res = std::move(col_res);

                    return true;
                }))
            {
                return res;
            }
            else
            {
                throw Exception(
                    "Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
            }
        }
        else
        {
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};


struct NameReinterpretAsUInt8       { static constexpr auto name = "reinterpretAsUInt8"; };
struct NameReinterpretAsUInt16      { static constexpr auto name = "reinterpretAsUInt16"; };
struct NameReinterpretAsUInt32      { static constexpr auto name = "reinterpretAsUInt32"; };
struct NameReinterpretAsUInt64      { static constexpr auto name = "reinterpretAsUInt64"; };
struct NameReinterpretAsInt8        { static constexpr auto name = "reinterpretAsInt8"; };
struct NameReinterpretAsInt16       { static constexpr auto name = "reinterpretAsInt16"; };
struct NameReinterpretAsInt32       { static constexpr auto name = "reinterpretAsInt32"; };
struct NameReinterpretAsInt64       { static constexpr auto name = "reinterpretAsInt64"; };
struct NameReinterpretAsFloat32     { static constexpr auto name = "reinterpretAsFloat32"; };
struct NameReinterpretAsFloat64     { static constexpr auto name = "reinterpretAsFloat64"; };
struct NameReinterpretAsDate        { static constexpr auto name = "reinterpretAsDate"; };
struct NameReinterpretAsDateTime    { static constexpr auto name = "reinterpretAsDateTime"; };
struct NameReinterpretAsUUID        { static constexpr auto name = "reinterpretAsUUID"; };

using FunctionReinterpretAsUInt8 = FunctionReinterpretAs<DataTypeUInt8, NameReinterpretAsUInt8, false>;
using FunctionReinterpretAsUInt16 = FunctionReinterpretAs<DataTypeUInt16, NameReinterpretAsUInt16, false>;
using FunctionReinterpretAsUInt32 = FunctionReinterpretAs<DataTypeUInt32, NameReinterpretAsUInt32, true>;
using FunctionReinterpretAsUInt64 = FunctionReinterpretAs<DataTypeUInt64, NameReinterpretAsUInt64, true>;
using FunctionReinterpretAsInt8 = FunctionReinterpretAs<DataTypeInt8, NameReinterpretAsInt8, false>;
using FunctionReinterpretAsInt16 = FunctionReinterpretAs<DataTypeInt16, NameReinterpretAsInt16, false>;
using FunctionReinterpretAsInt32 = FunctionReinterpretAs<DataTypeInt32, NameReinterpretAsInt32, true>;
using FunctionReinterpretAsInt64 = FunctionReinterpretAs<DataTypeInt64, NameReinterpretAsInt64, true>;
using FunctionReinterpretAsFloat32 = FunctionReinterpretAs<DataTypeFloat32, NameReinterpretAsFloat32, true>;
using FunctionReinterpretAsFloat64 = FunctionReinterpretAs<DataTypeFloat64, NameReinterpretAsFloat64, true>;
using FunctionReinterpretAsDate = FunctionReinterpretAs<DataTypeDate, NameReinterpretAsDate, false>;
using FunctionReinterpretAsDateTime = FunctionReinterpretAs<DataTypeDateTime, NameReinterpretAsDateTime, false>;
using FunctionReinterpretAsUUID = FunctionReinterpretAs<DataTypeUUID, NameReinterpretAsUUID, false>;
}

void registerFunctionsReinterpretAs(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReinterpretAsUInt8>();
    factory.registerFunction<FunctionReinterpretAsUInt16>();
    factory.registerFunction<FunctionReinterpretAsUInt32>();
    factory.registerFunction<FunctionReinterpretAsUInt64>();
    factory.registerFunction<FunctionReinterpretAsInt8>();
    factory.registerFunction<FunctionReinterpretAsInt16>();
    factory.registerFunction<FunctionReinterpretAsInt32>();
    factory.registerFunction<FunctionReinterpretAsInt64>();
    factory.registerFunction<FunctionReinterpretAsFloat32>();
    factory.registerFunction<FunctionReinterpretAsFloat64>();
    factory.registerFunction<FunctionReinterpretAsDate>();
    factory.registerFunction<FunctionReinterpretAsDateTime>();
    factory.registerFunction<FunctionReinterpretAsUUID>();
}

}
