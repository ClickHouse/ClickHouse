#include <Compression/chooseNetworkCompressionCodec.h>

#include <Compression/CompressionFactory.h>
#include <Core/Settings.h>
#include <Common/Exception.h>

#include <Poco/String.h>

namespace DB
{

namespace Setting
{
    extern const SettingsString network_compression_method;
    extern const SettingsInt64 network_zstd_compression_level;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

CompressionCodecPtr chooseNetworkCompressionCodec(const Settings * settings)
{
    if (!settings)
        return CompressionCodecFactory::instance().getDefaultCodec();

    std::optional<int> level;
    std::string method = Poco::toUpper((*settings)[Setting::network_compression_method].toString());

    /// Bad custom logic
    /// We only allow any of following generic codecs. CompressionCodecFactory will happily return other
    /// codecs (e.g. T64) but these may be specialized and not support all data types, i.e. SELECT 'abc' may
    /// be broken afterwards.
    if (method != "NONE" && method != "ZSTD" && method != "LZ4" && method != "LZ4HC")
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Setting 'network_compression_method' must be NONE, ZSTD, LZ4 or LZ4HC");

    /// More bad custom logic
    if (method == "ZSTD")
        level = (*settings)[Setting::network_zstd_compression_level];

    CompressionCodecFactory::instance().validateCodec(method, level, CodecValidationSettings(*settings));
    return CompressionCodecFactory::instance().get(method, level);
}

}
