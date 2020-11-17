#include <Functions/FunctionFactory.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>

#include <Common/typeid_cast.h>
#include <Common/memcpySmall.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename ToDataType, typename Name>
class FunctionReinterpretStringAs : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionReinterpretStringAs>(); }

    using ToFieldType = typename ToDataType::FieldType;
    using ColumnType = typename ToDataType::ColumnType;

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const IDataType & type = *arguments[0];
        if (!isStringOrFixedString(type))
            throw Exception("Cannot reinterpret " + type.getName() + " as " + ToDataType().getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<ToDataType>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        if (const ColumnString * col_from = typeid_cast<const ColumnString *>(block.getByPosition(arguments[0]).column.get()))
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
                ToFieldType value = 0;
                memcpy(&value, &data_from[offset], std::min(static_cast<UInt64>(sizeof(ToFieldType)), offsets_from[i] - offset - 1));
                vec_res[i] = value;
                offset = offsets_from[i];
            }

            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const ColumnFixedString * col_from_fixed = typeid_cast<const ColumnFixedString *>(block.getByPosition(arguments[0]).column.get()))
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
                ToFieldType value = 0;
                memcpy(&value, &data_from[offset], copy_size);
                vec_res[i] = value;
                offset += step;
            }

            block.getByPosition(result).column = std::move(col_res);
        }
        else
        {
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
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

using FunctionReinterpretAsUInt8 = FunctionReinterpretStringAs<DataTypeUInt8,       NameReinterpretAsUInt8>;
using FunctionReinterpretAsUInt16 = FunctionReinterpretStringAs<DataTypeUInt16,     NameReinterpretAsUInt16>;
using FunctionReinterpretAsUInt32 = FunctionReinterpretStringAs<DataTypeUInt32,     NameReinterpretAsUInt32>;
using FunctionReinterpretAsUInt64 = FunctionReinterpretStringAs<DataTypeUInt64,     NameReinterpretAsUInt64>;
using FunctionReinterpretAsInt8 = FunctionReinterpretStringAs<DataTypeInt8,         NameReinterpretAsInt8>;
using FunctionReinterpretAsInt16 = FunctionReinterpretStringAs<DataTypeInt16,       NameReinterpretAsInt16>;
using FunctionReinterpretAsInt32 = FunctionReinterpretStringAs<DataTypeInt32,       NameReinterpretAsInt32>;
using FunctionReinterpretAsInt64 = FunctionReinterpretStringAs<DataTypeInt64,       NameReinterpretAsInt64>;
using FunctionReinterpretAsFloat32 = FunctionReinterpretStringAs<DataTypeFloat32,   NameReinterpretAsFloat32>;
using FunctionReinterpretAsFloat64 = FunctionReinterpretStringAs<DataTypeFloat64,   NameReinterpretAsFloat64>;
using FunctionReinterpretAsDate = FunctionReinterpretStringAs<DataTypeDate,         NameReinterpretAsDate>;
using FunctionReinterpretAsDateTime = FunctionReinterpretStringAs<DataTypeDateTime, NameReinterpretAsDateTime>;


void registerFunctionsReinterpretStringAs(FunctionFactory & factory)
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
}

}


