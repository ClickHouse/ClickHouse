#include <Interpreters/Settings.h>

#include "CompressionSettings.h"


namespace DB
{

CompressionSettings::CompressionSettings()
{
}

CompressionSettings::CompressionSettings(CompressionMethod method):
    method(method)
{
}

CompressionSettings::CompressionSettings(CompressionMethod method, int zstd_level):
    method(method),
    zstd_level(zstd_level)
{
}

CompressionSettings::CompressionSettings(const Settings & settings):
    CompressionSettings(settings.network_compression_method, settings.network_zstd_compression_level)
{
}

}
