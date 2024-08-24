#include "config.h"

#if USE_SZ3

#    include "DataTypes/IDataType.h"

#    include <Compression/CompressionFactory.h>
#    include <Compression/CompressionInfo.h>
#    include <Compression/ICompressionCodec.h>
#    include <Core/Settings.h>
#    include <IO/BufferWithOwnMemory.h>
#    include <IO/WriteBuffer.h>
#    include <IO/WriteHelpers.h>
#    include <Interpreters/Context.h>
#    include <Parsers/ASTFunction.h>
#    include <Parsers/ASTIdentifier.h>
#    include <Parsers/ASTLiteral.h>
#    include <Parsers/IAST.h>
#    include <Common/CurrentThread.h>

#    include <SZ3/api/sz.hpp>

namespace DB
{

class CompressionCodecSZ3 : public ICompressionCodec
{
public:
    enum class DataType : uint8_t
    {
        FLOAT32 = 0,
        FLOAT64
    };

    explicit CompressionCodecSZ3(
        DataType type_,
        const std::string & compression_algo_,
        const std::string & error_bound_mode_,
        double rel_error_bound_,
        double abs_error_bound_,
        double psnr_error_bound_,
        double l2_norm_error_bound_);

    uint8_t getMethodByte() const override;

    UInt32 getAdditionalSizeAtTheEndOfBuffer() const override { return 0; }

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }
    bool isExperimental() const override { return true; }

private:
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    DataType type;

    std::string compression_algo;
    std::string error_bound_mode;
    double rel_error_bound;
    double abs_error_bound;
    double psnr_error_bound;
    double l2_norm_error_bound;
};

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_CODEC_PARAMETER;
}

CompressionCodecSZ3::CompressionCodecSZ3(
    DataType type_,
    const std::string & compression_algo_,
    const std::string & error_bound_mode_,
    double rel_error_bound_,
    double abs_error_bound_,
    double psnr_error_bound_,
    double l2_norm_error_bound_)
    : type(type_)
    , compression_algo(compression_algo_)
    , error_bound_mode(error_bound_mode_)
    , rel_error_bound(rel_error_bound_)
    , abs_error_bound(abs_error_bound_)
    , psnr_error_bound(psnr_error_bound_)
    , l2_norm_error_bound(l2_norm_error_bound_)
{
    setCodecDescription("SZ3");
}

uint8_t CompressionCodecSZ3::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::SZ3);
}

void CompressionCodecSZ3::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, true);
    hash.update(type);
}

UInt32 CompressionCodecSZ3::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return uncompressed_size + 1;
}

UInt32 CompressionCodecSZ3::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    char * res;
    size_t src_size;

    auto source_compressed = std::make_unique<char[]>(source_size);
    memcpy(source_compressed.get(), source, source_size);

    switch (type)
    {
        case DataType::FLOAT32: {
            src_size = static_cast<size_t>(source_size) / sizeof(float);
            dest[0] = 0;
            ++dest;
            break;
        }
        case DataType::FLOAT64: {
            src_size = static_cast<size_t>(source_size) / sizeof(double);
            dest[0] = 1;
            ++dest;
            break;
        }
    }

    SZ3::Config conf(src_size);

    int algorithm_position = -1;
    for (int i = 0; i < 4; ++i)
    {
        if (SZ3::ALGO_STR[i] == compression_algo)
        {
            algorithm_position = i;
        }
    }
    if (algorithm_position == -1)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect algorithm for sz3 codec {}", compression_algo);
    }

    int error_mode_position = -1;
    for (int i = 0; i < 4; ++i)
    {
        if (SZ3::EB_STR[i] == error_bound_mode)
        {
            error_mode_position = i;
        }
    }
    if (error_mode_position == -1)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect error mode for sz3 codec {}", error_bound_mode);
    }

    conf.cmprAlgo = SZ3::ALGO_OPTIONS[algorithm_position];
    conf.errorBoundMode = SZ3::EB_OPTIONS[error_mode_position];
    conf.relErrorBound = rel_error_bound;
    conf.absErrorBound = abs_error_bound;
    conf.psnrErrorBound = psnr_error_bound;
    conf.l2normErrorBound = l2_norm_error_bound;

    switch (type)
    {
        case DataType::FLOAT32: {
            res = SZ_compress(conf, reinterpret_cast<float *>(source_compressed.get()), src_size);
            break;
        }
        case DataType::FLOAT64: {
            res = SZ_compress(conf, reinterpret_cast<double *>(source_compressed.get()), src_size);
            break;
        }
    }

    memcpy(dest, res, src_size);
    return static_cast<UInt32>(src_size) + 1;
}

void CompressionCodecSZ3::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    SZ3::Config conf;
    --source_size;
    DataType data_type = static_cast<DataType>(source[0]);
    ++source;
    auto copy_compressed = std::make_unique<char[]>(source_size);
    memcpy(copy_compressed.get(), source, source_size);

    switch (data_type)
    {
        case DataType::FLOAT32: {
            auto * res = SZ_decompress<float>(conf, copy_compressed.get(), source_size);
            memcpy(dest, reinterpret_cast<char *>(res), static_cast<size_t>(uncompressed_size));
            break;
        }
        case DataType::FLOAT64: {
            auto * res = SZ_decompress<double>(conf, copy_compressed.get(), source_size);
            memcpy(dest, reinterpret_cast<char *>(res), static_cast<size_t>(uncompressed_size));
            break;
        }
    }
}

CompressionCodecSZ3::DataType getDataType(const IDataType & column_type)
{
    switch (WhichDataType(column_type).idx)
    {
        case TypeIndex::Float32:
            return CompressionCodecSZ3::DataType::FLOAT32;
        case TypeIndex::Float64:
            return CompressionCodecSZ3::DataType::FLOAT64;
        default:
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "SZ3 codec is not applicable for {} because the data type must be Float32 or Float64",
                column_type.getName());
    }
}

void registerCodecSZ3(CompressionCodecFactory & factory)
{
    auto method_code = static_cast<UInt8>(CompressionMethodByte::SZ3);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        auto data_type = CompressionCodecSZ3::DataType::FLOAT32;
        if (column_type)
        {
            data_type = getDataType(*column_type);
        }

        if (!arguments || arguments->children.size() == 0)
        {
            return std::make_shared<CompressionCodecSZ3>(data_type, "ALGO_INTERP_LORENZO", "REL", 1e-2, 0, 0, 0);
        }
        else if (arguments->children.size() == 3)
        {
            const auto children = arguments->children;
            const auto * literal_algorithm = children[0]->as<ASTLiteral>();
            if (!literal_algorithm)
            {
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "SZ3 codec argument 0 must be string");
            }
            auto algorithm_string = static_cast<std::string>(literal_algorithm->value.safeGet<String>());

            const auto * literal_error_mode = children[1]->as<ASTLiteral>();
            if (!literal_error_mode)
            {
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "SZ3 codec argument 1 must be string");
            }
            auto error_mode = static_cast<std::string>(literal_error_mode->value.safeGet<String>());

            const auto * literal_error_value = children[2]->as<ASTLiteral>();
            if (!literal_error_value)
            {
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "SZ3 codec argument 2 must be double");
            }
            auto error_value = static_cast<double>(literal_error_value->value.safeGet<Float64>());

            if (error_mode == "REL")
            {
                return std::make_shared<CompressionCodecSZ3>(data_type, algorithm_string, error_mode, error_value, 0, 0, 0);
            }
            else if (error_mode == "ABS")
            {
                return std::make_shared<CompressionCodecSZ3>(data_type, algorithm_string, error_mode, 0, error_value, 0, 0);
            }
            else if (error_mode == "PSNR")
            {
                return std::make_shared<CompressionCodecSZ3>(data_type, algorithm_string, error_mode, 0, 0, error_value, 0);
            }
            else if (error_mode == "NORM")
            {
                return std::make_shared<CompressionCodecSZ3>(data_type, algorithm_string, error_mode, 0, 0, 0, error_value);
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect error mode for sz3 codec {}", error_mode);
            }
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect number of arguments {}", arguments->children.size());
        }
    };
    factory.registerCompressionCodecWithType("SZ3", method_code, codec_builder);
}

}
#endif
