#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsTransform.h>
#include <DataTypes/DataTypeTraits.h>

namespace DB
{

namespace
{

template <typename TLeft, typename TRight, typename TType>
struct TypeProcessorImpl
{
    static DataTypeTraits::EnrichedDataTypePtr execute()
    {
        using EnrichedT1 = std::tuple<TLeft, TRight, NumberTraits::HasNoNull>;
        using EnrichedT2 = typename NumberTraits::EmbedType<TType>::Type;
        using TCombined = typename NumberTraits::TypeProduct<EnrichedT1, EnrichedT2>::Type;

        auto type_res = DataTypeTraits::ToEnrichedDataTypeObject<TCombined, true>::execute();
        if ((type_res.first == DataTypePtr()) && (type_res.second == DataTypePtr()))
            throw Exception("Types " + String(TypeName<TLeft>::get()) + " and " + String(TypeName<TType>::get())
                + " are not upscalable to a common type without loss of precision", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return type_res;
    }
};

template <typename TLeft, typename TRight>
struct RightTypeProcessor
{
    static DataTypeTraits::EnrichedDataTypePtr execute(const IDataType & type2)
    {
        if (checkDataType<DataTypeUInt8>(&type2))   return TypeProcessorImpl<TLeft, TRight, UInt8>::execute();
        if (checkDataType<DataTypeUInt16>(&type2))  return TypeProcessorImpl<TLeft, TRight, UInt16>::execute();
        if (checkDataType<DataTypeUInt32>(&type2))  return TypeProcessorImpl<TLeft, TRight, UInt32>::execute();
        if (checkDataType<DataTypeUInt64>(&type2))  return TypeProcessorImpl<TLeft, TRight, UInt64>::execute();
        if (checkDataType<DataTypeInt8>(&type2))    return TypeProcessorImpl<TLeft, TRight, Int8>::execute();
        if (checkDataType<DataTypeInt16>(&type2))   return TypeProcessorImpl<TLeft, TRight, Int16>::execute();
        if (checkDataType<DataTypeInt32>(&type2))   return TypeProcessorImpl<TLeft, TRight, Int32>::execute();
        if (checkDataType<DataTypeInt64>(&type2))   return TypeProcessorImpl<TLeft, TRight, Int64>::execute();
        if (checkDataType<DataTypeFloat32>(&type2)) return TypeProcessorImpl<TLeft, TRight, Float32>::execute();
        if (checkDataType<DataTypeFloat64>(&type2)) return TypeProcessorImpl<TLeft, TRight, Float64>::execute();

        throw Exception("Logical error: not a numeric type passed to function getSmallestCommonNumericType", ErrorCodes::LOGICAL_ERROR);
    }
};

template <typename TLeft>
struct LeftTypeProcessor
{
    static DataTypeTraits::EnrichedDataTypePtr execute(const DataTypePtr & right, const IDataType & type2)
    {
        if (checkDataType<DataTypeVoid>(&*right))    return RightTypeProcessor<TLeft, void>::execute(type2);
        if (checkDataType<DataTypeUInt8>(&*right))   return RightTypeProcessor<TLeft, UInt8>::execute(type2);
        if (checkDataType<DataTypeUInt16>(&*right))  return RightTypeProcessor<TLeft, UInt16>::execute(type2);
        if (checkDataType<DataTypeUInt32>(&*right))  return RightTypeProcessor<TLeft, UInt32>::execute(type2);
        if (checkDataType<DataTypeUInt64>(&*right))  return RightTypeProcessor<TLeft, UInt64>::execute(type2);
        if (checkDataType<DataTypeInt8>(&*right))    return RightTypeProcessor<TLeft, Int8>::execute(type2);
        if (checkDataType<DataTypeInt16>(&*right))   return RightTypeProcessor<TLeft, Int16>::execute(type2);
        if (checkDataType<DataTypeInt32>(&*right))   return RightTypeProcessor<TLeft, Int32>::execute(type2);
        if (checkDataType<DataTypeInt64>(&*right))   return RightTypeProcessor<TLeft, Int64>::execute(type2);
        if (checkDataType<DataTypeFloat32>(&*right)) return RightTypeProcessor<TLeft, Float32>::execute(type2);
        if (checkDataType<DataTypeFloat64>(&*right)) return RightTypeProcessor<TLeft, Float64>::execute(type2);

        throw Exception("Logical error: not a numeric type passed to function getSmallestCommonNumericType", ErrorCodes::LOGICAL_ERROR);
    }
};

}

DataTypeTraits::EnrichedDataTypePtr getSmallestCommonNumericType(const DataTypeTraits::EnrichedDataTypePtr & type1, const IDataType & type2)
{
    const DataTypePtr & left = type1.first;
    const DataTypePtr & right = type1.second;

    if (checkDataType<DataTypeUInt8>(&*left))   return LeftTypeProcessor<UInt8>::execute(right, type2);
    if (checkDataType<DataTypeUInt16>(&*left))  return LeftTypeProcessor<UInt16>::execute(right, type2);
    if (checkDataType<DataTypeUInt32>(&*left))  return LeftTypeProcessor<UInt32>::execute(right, type2);
    if (checkDataType<DataTypeUInt64>(&*left))  return LeftTypeProcessor<UInt64>::execute(right, type2);
    if (checkDataType<DataTypeInt8>(&*left))    return LeftTypeProcessor<Int8>::execute(right, type2);
    if (checkDataType<DataTypeInt16>(&*left))   return LeftTypeProcessor<Int16>::execute(right, type2);
    if (checkDataType<DataTypeInt32>(&*left))   return LeftTypeProcessor<Int32>::execute(right, type2);
    if (checkDataType<DataTypeInt64>(&*left))   return LeftTypeProcessor<Int64>::execute(right, type2);
    if (checkDataType<DataTypeFloat32>(&*left)) return LeftTypeProcessor<Float32>::execute(right, type2);
    if (checkDataType<DataTypeFloat64>(&*left)) return LeftTypeProcessor<Float64>::execute(right, type2);

    throw Exception("Logical error: not a numeric type passed to function getSmallestCommonNumericType", ErrorCodes::LOGICAL_ERROR);
}

}


namespace DB
{

void registerFunctionsTransform(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTransform>();
}

}
