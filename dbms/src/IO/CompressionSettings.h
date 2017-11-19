#pragma once

#include <IO/CompressedStream.h>


namespace DB
{

struct Settings;

struct CompressionSettings
{
    CompressionMethod method;
    int level;

    CompressionSettings()
        : CompressionSettings(CompressionMethod::LZ4)
    {
    }

    CompressionSettings(CompressionMethod method_)
        : method(method_)
        , level(getDefaultLevel(method))
    {
    }

    CompressionSettings(CompressionMethod method_, int level_)
        : method(method_)
        , level(level_)
    {
    }

    CompressionSettings(const Settings & settings);

    static int getDefaultLevel(CompressionMethod method);
};

}
