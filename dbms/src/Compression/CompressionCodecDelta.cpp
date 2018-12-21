#include <Compression/CompressionCodecDelta.h>
#include <Parsers/IAST.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>
#include <boost/mpl/for_each.hpp>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_CODEC_PARAMETER;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
}

template<typename WidthType>
UInt8 CompressionCodecDelta<WidthType>::getMethodByte() const
{
    return static_cast<UInt8>(CompressionMethodByte::Delta);
}

template<typename WidthType>
String CompressionCodecDelta<WidthType>::getCodecDesc() const
{
    return String("Delta(") + delta_type->getFamilyName() + ")";
}

template<typename WidthType>
UInt32 CompressionCodecDelta<WidthType>::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if (source_size % sizeof(WidthType) != 0)
        throw Exception("Logical error: failure to use delta codec for data.", ErrorCodes::LOGICAL_ERROR);

    auto * dest_with_type = reinterpret_cast<WidthType *>(dest);
    const auto * source_with_type = reinterpret_cast<const WidthType *>(source);

    /// TODO: SIMD
    if (source_size > 0)
        dest_with_type[0] = source_with_type[0];

    for (size_t dest_index = 1, dest_end = source_size / sizeof(WidthType); dest_index < dest_end; ++dest_index)
        dest_with_type[dest_index] = source_with_type[dest_index] - source_with_type[dest_index - 1];

    return source_size;
}

template<typename WidthType>
void CompressionCodecDelta<WidthType>::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 /*uncompressed_size*/) const
{
    auto * dest_with_type = reinterpret_cast<WidthType *>(dest);
    const auto * source_with_type = reinterpret_cast<const WidthType *>(source);

    if (source_size > 0)
        dest_with_type[0] = source_with_type[0];

    for (size_t dest_index = 1, dest_end = source_size / sizeof(WidthType); dest_index < dest_end; ++dest_index)
        dest_with_type[dest_index] = source_with_type[dest_index] + dest_with_type[dest_index - 1];
}

template<typename WidthType>
CompressionCodecDelta<WidthType>::CompressionCodecDelta(const DataTypePtr & delta_type)
    :delta_type(delta_type)
{
}

template <typename T, class... Ns>
CompressionCodecPtr deltaCodecCreatorImpl(const DataTypePtr & data_type)
{
    if (typeid_cast<const DataTypeNumber<T> *>(data_type.get())
        || typeid_cast<const DataTypeEnum<T> *>(data_type.get())
        || dynamic_cast<const DataTypeNumberBase<T> *>(data_type.get()))
        return std::make_shared<CompressionCodecDelta<T>>(data_type);

    if constexpr (sizeof...(Ns) != 0)
        return deltaCodecCreatorImpl<Ns...>(data_type);

    /// other cases: use the delta codec with 1 byte width
    return std::make_shared<CompressionCodecDelta<UInt8>>(data_type);
}

void registerCodecDelta(CompressionCodecFactory & factory)
{
    factory.registerCompressionCodec("Delta", static_cast<char>(CompressionMethodByte::Delta), [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        if (!arguments || arguments->children.size() != 1)
            throw Exception("Delta codec must have 1 parameter, given " + std::to_string(arguments->children.size()), ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

        DataTypePtr delta_type = DataTypeFactory::instance().get(arguments->children[0]);

        if (!delta_type->isValueRepresentedByInteger() && !WhichDataType(delta_type).isStringOrFixedString())
            throw Exception("The delta codec is only support Integer, Date, DateTime, String, FixedString data types",
                            ErrorCodes::ILLEGAL_CODEC_PARAMETER);

        return deltaCodecCreatorImpl<Int64, Int32, Int16, Int8, UInt64, UInt32, UInt16, UInt8>(delta_type);
    });
}

}
