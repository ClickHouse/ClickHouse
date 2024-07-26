#ifdef ENABLE_SZ3

#include "DataTypes/IDataType.h"

#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/BufferWithOwnMemory.h>
#include <Core/Settings.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>

#include <SZ3/api/sz.hpp>

namespace DB
{

class CompressionCodecSZ3 : public ICompressionCodec
{
public:
    enum class DataType : uint8_t
    {
        FLOAT32 = 0,
        FLOAT64 = 1,
    };

    explicit CompressionCodecSZ3(DataType type_);

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
};

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

CompressionCodecSZ3::CompressionCodecSZ3(DataType type_)
    : type(type_)
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
    return uncompressed_size;
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
                "SZ3 codec is not applicable for {} because the data type must be one of these: Float32, Float64",
                column_type.getName());
    }
}

UInt32 CompressionCodecSZ3::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    char* res;
    size_t src_size;

    char* source_compressed = new char[source_size];
    memcpy(source_compressed, source, source_size);

    switch (type)
    {
        case DataType::FLOAT32:
        {
            src_size = static_cast<size_t>(source_size) / sizeof(float);
            break;
        }
        case DataType::FLOAT64:
        {
            src_size = static_cast<size_t>(source_size) / sizeof(double);
            break;
        }
    }

    SZ3::Config conf(src_size);
    ContextPtr query_context;
    if (CurrentThread::isInitialized())
        query_context = CurrentThread::get().getQueryContext();

    if (!query_context)
    {
        // Default settings
        conf.cmprAlgo = SZ3::ALGO_INTERP_LORENZO;
        conf.errorBoundMode = SZ3::EB_REL;
        conf.relErrorBound = 1E-2;
    }
    else
    {
        conf.cmprAlgo = SZ3::ALGO_OPTIONS[query_context->getSettingsRef().sz3_algorithm];
        conf.errorBoundMode = SZ3::EB_OPTIONS[query_context->getSettingsRef().sz3_error_bound_mode];
        conf.relErrorBound = query_context->getSettingsRef().sz3_rel_error_bound;
        conf.absErrorBound = query_context->getSettingsRef().sz3_abs_error_bound;
        conf.psnrErrorBound = query_context->getSettingsRef().sz3_psnr_error_bound;
        conf.l2normErrorBound = query_context->getSettingsRef().sz3_l2_norm_error_bound;
    }

    switch (type)
    {
        case DataType::FLOAT32:
        {
            res = SZ_compress(conf, reinterpret_cast<float*>(source_compressed), src_size);
            break;
        }
        case DataType::FLOAT64:
        {
            res = SZ_compress(conf, reinterpret_cast<double*>(source_compressed), src_size);
            break;
        }
    }


    memcpy(dest, res, src_size);
    delete[] source_compressed;
    return static_cast<UInt32>(src_size);
}

void CompressionCodecSZ3::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    SZ3::Config conf;
    char* copy_compressed = new char[source_size];
    memcpy(copy_compressed, source, source_size);
    switch (type)
    {
        case DataType::FLOAT32:
        {
            auto* res = reinterpret_cast<float*>(dest);
            res = SZ_decompress<float>(conf, copy_compressed, source_size);
            memcpy(dest, reinterpret_cast<char*>(res), static_cast<size_t>(uncompressed_size));
            break;
        }
        case DataType::FLOAT64:
        {
            auto* res = reinterpret_cast<double*>(dest);
            res = SZ_decompress<double>(conf, copy_compressed, source_size);
            memcpy(dest, reinterpret_cast<char*>(res), static_cast<size_t>(uncompressed_size));
            break;
        }
    }
    delete[] copy_compressed;
}

void registerCodecSZ3(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::SZ3);

    auto reg_func = [&](const ASTPtr &, const IDataType * column_type) -> CompressionCodecPtr
    {
        auto data_type = CompressionCodecSZ3::DataType::FLOAT32;
        if (column_type)
        {
            data_type = getDataType(*column_type);
        }
        return std::make_shared<CompressionCodecSZ3>(data_type);
    };
    factory.registerCompressionCodecWithType("SZ3", method_code, reg_func);

}

}
#endif // ENABLE_SZ3
