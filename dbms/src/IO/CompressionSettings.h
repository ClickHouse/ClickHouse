#pragma once

#include <IO/CompressedStream.h>


namespace DB
{

class Settings;

struct CompressionSettings
{
    CompressionMethod method = CompressionMethod::LZ4;
    int zstd_level = 1;

    CompressionSettings();
    CompressionSettings(CompressionMethod method);
    CompressionSettings(CompressionMethod method, int zstd_level);
    CompressionSettings(const Settings & settings);
};

}
