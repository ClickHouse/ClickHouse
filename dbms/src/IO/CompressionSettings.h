#pragma once

#include <IO/CompressedStream.h>


namespace DB
{

class Settings;

struct CompressionSettings
{
    CompressionMethod method = CompressionMethod::LZ4;
    int level;

    CompressionSettings();
    CompressionSettings(CompressionMethod method);
    CompressionSettings(CompressionMethod method, int level);
    CompressionSettings(const Settings & settings);

    static int getDefaultLevel(CompressionMethod method);
};

}
