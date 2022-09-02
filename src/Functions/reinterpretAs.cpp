#include <Functions/FunctionFactory.h>
#include <Functions/castTypeToEither.h>
#include <Functions/FunctionHelpers.h>

#include <Core/callOnTypeIndex.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>

#include <Common/typeid_cast.h>
#include <Common/memcpySmall.h>

#include <base/unaligned.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** Performs byte reinterpretation similar to reinterpret_cast.
 *
 * Following reinterpretations are allowed:
 * 1. Any type that isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion into FixedString.
 * 2. Any type that isValueUnambiguouslyRepresentedInContiguousMemoryRegion into String.
 * 3. Types that can be interpreted as numeric (Integers, Float, Date, DateTime, UUID) into FixedString,
 * String, and types that can be interpreted as numeric (Integers, Float, Date, DateTime, UUID).
 */
class FunctionReinterpret : public IFunction
{
public:
    static constexpr auto name = "reinterpret";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionReinterpret>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto & column = arguments.back().column;

        DataTypePtr from_type = arguments[0].type;

        const auto * type_col = checkAndGetColumnConst<ColumnString>(column.get());
        if (!type_col)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument to {} must be a constant string describing type."
                " Instead there is non-constant column of type {}",
                getName(),
                arguments.back().type->getName());

        DataTypePtr to_type = DataTypeFactory::instance().get(type_col->getValue<String>());

        WhichDataType result_reinterpret_type(to_type);

        if (result_reinterpret_type.isFixedString())
        {
            if (!from_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot reinterpret {} as FixedString because it is not fixed size and contiguous in memory",
                    from_type->getName());
        }
        else if (result_reinterpret_type.isString())
        {
            if (!from_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot reinterpret {} as String because it is not contiguous in memory",
                    from_type->getName());
        }
        else if (canBeReinterpretedAsNumeric(result_reinterpret_type))
        {
            WhichDataType from_data_type(from_type);

            if (!canBeReinterpretedAsNumeric(from_data_type) && !from_data_type.isStringOrFixedString())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot reinterpret {} as {} because only Numeric, String or FixedString can be reinterpreted in Numeric",
                    from_type->getName(),
                    to_type->getName());
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Cannot reinterpret {} as {} because only reinterpretation in String, FixedString and Numeric types is supported",
                from_type->getName(),
                to_type->getName());
        }

        return to_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        auto from_type = arguments[0].type;

        ColumnPtr result;

        if (!callOnTwoTypeIndexes(from_type->getTypeId(), result_type->getTypeId(), [&](const auto & types)
        {
            using Types = std::decay_t<decltype(types)>;
            using FromType = typename Types::LeftType;
            using ToType = typename Types::RightType;

            /// Place this check before std::is_same_v<FromType, ToType> because same FixedString
            /// types does not necessary have the same byte size fixed value.
            if constexpr (std::is_same_v<ToType, DataTypeFixedString>)
            {
                const IColumn & src = *arguments[0].column;
                MutableColumnPtr dst = result_type->createColumn();

                ColumnFixedString * dst_concrete = assert_cast<ColumnFixedString *>(dst.get());

                if (src.isFixedAndContiguous() && src.sizeOfValueIfFixed() == dst_concrete->getN())
                    executeContiguousToFixedString(src, *dst_concrete, dst_concrete->getN());
                else
                    executeToFixedString(src, *dst_concrete, dst_concrete->getN());

                result = std::move(dst);

                return true;
            }
            else if constexpr (std::is_same_v<FromType, ToType>)
            {
                result = arguments[0].column;

                return true;
            }
            else if constexpr (std::is_same_v<ToType, DataTypeString>)
            {
                const IColumn & src = *arguments[0].column;
                MutableColumnPtr dst = result_type->createColumn();

                ColumnString * dst_concrete = assert_cast<ColumnString *>(dst.get());
                executeToString(src, *dst_concrete);

                result = std::move(dst);

                return true;
            }
            else if constexpr (CanBeReinterpretedAsNumeric<ToType>)
            {
                using ToFieldType = typename ToType::FieldType;

                if constexpr (std::is_same_v<FromType, DataTypeString>)
                {
                    const auto * col_from = assert_cast<const ColumnString *>(arguments[0].column.get());

                    auto col_res = numericColumnCreateHelper<ToType>(static_cast<const ToType&>(*result_type.get()));

                    const auto & data_from = col_from->getChars();
                    const auto & offsets_from = col_from->getOffsets();
                    size_t size = offsets_from.size();
                    auto & vec_res = col_res->getData();
                    vec_res.resize_fill(size);

                    size_t offset = 0;
                    for (size_t i = 0; i < size; ++i)
                    {
                        memcpy(&vec_res[i],
                            &data_from[offset],
                            std::min(static_cast<UInt64>(sizeof(ToFieldType)), offsets_from[i] - offset - 1));
                        offset = offsets_from[i];
                    }

                    result = std::move(col_res);

                    return true;
                }
                else if constexpr (std::is_same_v<FromType, DataTypeFixedString>)
                {
                    const auto * col_from_fixed = assert_cast<const ColumnFixedString *>(arguments[0].column.get());

                    auto col_res = numericColumnCreateHelper<ToType>(static_cast<const ToType&>(*result_type.get()));

                    const auto& data_from = col_from_fixed->getChars();
                    size_t step = col_from_fixed->getN();
                    size_t size = data_from.size() / step;
                    auto & vec_res = col_res->getData();

                    size_t offset = 0;
                    size_t copy_size = std::min(step, sizeof(ToFieldType));

                    if (sizeof(ToFieldType) <= step)
                        vec_res.resize(size);
                    else
                        vec_res.resize_fill(size);

                    for (size_t i = 0; i < size; ++i)
                    {
                        memcpy(&vec_res[i], &data_from[offset], copy_size);
                        offset += step;
                    }

                    result = std::move(col_res);

                    return true;
                }
                else if constexpr (CanBeReinterpretedAsNumeric<FromType>)
                {
                    using From = typename FromType::FieldType;
                    using To = typename ToType::FieldType;

                    using FromColumnType = ColumnVectorOrDecimal<From>;

                    const auto * column_from = assert_cast<const FromColumnType*>(arguments[0].column.get());

                    auto column_to = numericColumnCreateHelper<ToType>(static_cast<const ToType&>(*result_type.get()));

                    auto & from = column_from->getData();
                    auto & to = column_to->getData();

                    size_t size = from.size();
                    to.resize_fill(size);

                    static constexpr size_t copy_size = std::min(sizeof(From), sizeof(To));

                    for (size_t i = 0; i < size; ++i)
                        memcpy(static_cast<void*>(&to[i]), static_cast<const void*>(&from[i]), copy_size);

                    result = std::move(column_to);

                    return true;
                }
            }

            return false;
        }))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Cannot reinterpret {} as {}",
                from_type->getName(),
                result_type->getName());
        }

        return result;
    }
private:
    template <typename T>
    static constexpr auto CanBeReinterpretedAsNumeric =
        IsDataTypeDecimalOrNumber<T> ||
        std::is_same_v<T, DataTypeDate> ||
        std::is_same_v<T, DataTypeDateTime> ||
        std::is_same_v<T, DataTypeDateTime64> ||
        std::is_same_v<T, DataTypeUUID>;

    static bool canBeReinterpretedAsNumeric(const WhichDataType & type)
    {
        return type.isUInt() ||
            type.isInt() ||
            type.isDate() ||
            type.isDateTime() ||
            type.isDateTime64() ||
            type.isFloat() ||
            type.isUUID() ||
            type.isDecimal();
    }

    static void NO_INLINE executeToFixedString(const IColumn & src, ColumnFixedString & dst, size_t n)
    {
        size_t rows = src.size();
        ColumnFixedString::Chars & data_to = dst.getChars();
        data_to.resize_fill(n * rows);

        ColumnFixedString::Offset offset = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            StringRef data = src.getDataAt(i);

            memcpy(&data_to[offset], data.data, std::min(n, data.size));
            offset += n;
        }
    }

    static void NO_INLINE executeContiguousToFixedString(const IColumn & src, ColumnFixedString & dst, size_t n)
    {
        size_t rows = src.size();
        ColumnFixedString::Chars & data_to = dst.getChars();
        data_to.resize(n * rows);

        memcpy(data_to.data(), src.getRawData().data, data_to.size());
    }

    static void NO_INLINE executeToString(const IColumn & src, ColumnString & dst)
    {
        size_t rows = src.size();
        ColumnString::Chars & data_to = dst.getChars();
        ColumnString::Offsets & offsets_to = dst.getOffsets();
        offsets_to.resize(rows);

        ColumnString::Offset offset = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            StringRef data = src.getDataAt(i);

            /// Cut trailing zero bytes.
            while (data.size && data.data[data.size - 1] == 0)
                --data.size;

            data_to.resize(offset + data.size + 1);
            memcpy(&data_to[offset], data.data, data.size);
            offset += data.size;
            data_to[offset] = 0;
            ++offset;
            offsets_to[i] = offset;
        }
    }

    template <typename Type>
    static typename Type::ColumnType::MutablePtr numericColumnCreateHelper(const Type & type)
    {
        size_t column_size = 0;

        using ColumnType = typename Type::ColumnType;

        if constexpr (IsDataTypeDecimal<Type>)
            return ColumnType::create(column_size, type.getScale());
        else
            return ColumnType::create(column_size);
    }

    template <typename FromContainer, typename ToContainer>
    static void reinterpretImpl(const FromContainer & from, ToContainer & to)
    {
        using From = typename FromContainer::value_type;
        using To = typename ToContainer::value_type;

        size_t size = from.size();
        static constexpr size_t copy_size = std::min(sizeof(From), sizeof(To));

        if (sizeof(To) <= sizeof(From))
            to.resize(size);
        else
            to.resize_fill(size);

        for (size_t i = 0; i < size; ++i)
            memcpy(static_cast<void*>(&to[i]), static_cast<const void*>(&from[i]), copy_size);
    }
};

template <typename ToDataType, typename Name>
class FunctionReinterpretAs : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionReinterpretAs>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return impl.useDefaultImplementationForConstants(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        return impl.isSuitableForShortCircuitArgumentsExecution(arguments);
    }

    static ColumnsWithTypeAndName addTypeColumnToArguments(const ColumnsWithTypeAndName & arguments)
    {
        const auto & argument = arguments[0];

        DataTypePtr data_type;

        if constexpr (std::is_same_v<ToDataType, DataTypeFixedString>)
        {
            const auto & type = argument.type;

            if (!type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot reinterpret {} as FixedString because it is not fixed size and contiguous in memory",
                    type->getName());

            size_t type_value_size_in_memory = type->getSizeOfValueInMemory();
            data_type = std::make_shared<DataTypeFixedString>(type_value_size_in_memory);
        }
        else
            data_type = std::make_shared<ToDataType>();

        auto type_name_column = DataTypeString().createColumnConst(1, data_type->getName());
        ColumnWithTypeAndName type_column(type_name_column, std::make_shared<DataTypeString>(), "");

        ColumnsWithTypeAndName arguments_with_type
        {
            argument,
            type_column
        };

        return arguments_with_type;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto arguments_with_type = addTypeColumnToArguments(arguments);
        return impl.getReturnTypeImpl(arguments_with_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const override
    {
        auto arguments_with_type = addTypeColumnToArguments(arguments);
        return impl.executeImpl(arguments_with_type, return_type, input_rows_count);
    }

    FunctionReinterpret impl;
};

struct NameReinterpretAsUInt8       { static constexpr auto name = "reinterpretAsUInt8"; };
struct NameReinterpretAsUInt16      { static constexpr auto name = "reinterpretAsUInt16"; };
struct NameReinterpretAsUInt32      { static constexpr auto name = "reinterpretAsUInt32"; };
struct NameReinterpretAsUInt64      { static constexpr auto name = "reinterpretAsUInt64"; };
struct NameReinterpretAsUInt128     { static constexpr auto name = "reinterpretAsUInt128"; };
struct NameReinterpretAsUInt256     { static constexpr auto name = "reinterpretAsUInt256"; };
struct NameReinterpretAsInt8        { static constexpr auto name = "reinterpretAsInt8"; };
struct NameReinterpretAsInt16       { static constexpr auto name = "reinterpretAsInt16"; };
struct NameReinterpretAsInt32       { static constexpr auto name = "reinterpretAsInt32"; };
struct NameReinterpretAsInt64       { static constexpr auto name = "reinterpretAsInt64"; };
struct NameReinterpretAsInt128      { static constexpr auto name = "reinterpretAsInt128"; };
struct NameReinterpretAsInt256      { static constexpr auto name = "reinterpretAsInt256"; };
struct NameReinterpretAsFloat32     { static constexpr auto name = "reinterpretAsFloat32"; };
struct NameReinterpretAsFloat64     { static constexpr auto name = "reinterpretAsFloat64"; };
struct NameReinterpretAsDate        { static constexpr auto name = "reinterpretAsDate"; };
struct NameReinterpretAsDateTime    { static constexpr auto name = "reinterpretAsDateTime"; };
struct NameReinterpretAsUUID        { static constexpr auto name = "reinterpretAsUUID"; };
struct NameReinterpretAsString      { static constexpr auto name = "reinterpretAsString"; };
struct NameReinterpretAsFixedString { static constexpr auto name = "reinterpretAsFixedString"; };

using FunctionReinterpretAsUInt8 = FunctionReinterpretAs<DataTypeUInt8, NameReinterpretAsUInt8>;
using FunctionReinterpretAsUInt16 = FunctionReinterpretAs<DataTypeUInt16, NameReinterpretAsUInt16>;
using FunctionReinterpretAsUInt32 = FunctionReinterpretAs<DataTypeUInt32, NameReinterpretAsUInt32>;
using FunctionReinterpretAsUInt64 = FunctionReinterpretAs<DataTypeUInt64, NameReinterpretAsUInt64>;
using FunctionReinterpretAsUInt128 = FunctionReinterpretAs<DataTypeUInt128, NameReinterpretAsUInt128>;
using FunctionReinterpretAsUInt256 = FunctionReinterpretAs<DataTypeUInt256, NameReinterpretAsUInt256>;
using FunctionReinterpretAsInt8 = FunctionReinterpretAs<DataTypeInt8, NameReinterpretAsInt8>;
using FunctionReinterpretAsInt16 = FunctionReinterpretAs<DataTypeInt16, NameReinterpretAsInt16>;
using FunctionReinterpretAsInt32 = FunctionReinterpretAs<DataTypeInt32, NameReinterpretAsInt32>;
using FunctionReinterpretAsInt64 = FunctionReinterpretAs<DataTypeInt64, NameReinterpretAsInt64>;
using FunctionReinterpretAsInt128 = FunctionReinterpretAs<DataTypeInt128, NameReinterpretAsInt128>;
using FunctionReinterpretAsInt256 = FunctionReinterpretAs<DataTypeInt256, NameReinterpretAsInt256>;
using FunctionReinterpretAsFloat32 = FunctionReinterpretAs<DataTypeFloat32, NameReinterpretAsFloat32>;
using FunctionReinterpretAsFloat64 = FunctionReinterpretAs<DataTypeFloat64, NameReinterpretAsFloat64>;
using FunctionReinterpretAsDate = FunctionReinterpretAs<DataTypeDate, NameReinterpretAsDate>;
using FunctionReinterpretAsDateTime = FunctionReinterpretAs<DataTypeDateTime, NameReinterpretAsDateTime>;
using FunctionReinterpretAsUUID = FunctionReinterpretAs<DataTypeUUID, NameReinterpretAsUUID>;

using FunctionReinterpretAsString = FunctionReinterpretAs<DataTypeString, NameReinterpretAsString>;

using FunctionReinterpretAsFixedString = FunctionReinterpretAs<DataTypeFixedString, NameReinterpretAsFixedString>;

}

void registerFunctionsReinterpretAs(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReinterpretAsUInt8>();
    factory.registerFunction<FunctionReinterpretAsUInt16>();
    factory.registerFunction<FunctionReinterpretAsUInt32>();
    factory.registerFunction<FunctionReinterpretAsUInt64>();
    factory.registerFunction<FunctionReinterpretAsUInt128>();
    factory.registerFunction<FunctionReinterpretAsUInt256>();
    factory.registerFunction<FunctionReinterpretAsInt8>();
    factory.registerFunction<FunctionReinterpretAsInt16>();
    factory.registerFunction<FunctionReinterpretAsInt32>();
    factory.registerFunction<FunctionReinterpretAsInt64>();
    factory.registerFunction<FunctionReinterpretAsInt128>();
    factory.registerFunction<FunctionReinterpretAsInt256>();
    factory.registerFunction<FunctionReinterpretAsFloat32>();
    factory.registerFunction<FunctionReinterpretAsFloat64>();
    factory.registerFunction<FunctionReinterpretAsDate>();
    factory.registerFunction<FunctionReinterpretAsDateTime>();
    factory.registerFunction<FunctionReinterpretAsUUID>();

    factory.registerFunction<FunctionReinterpretAsString>();

    factory.registerFunction<FunctionReinterpretAsFixedString>();

    factory.registerFunction<FunctionReinterpret>();
}

}
