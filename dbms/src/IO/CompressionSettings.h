#pragma once

#include <Compression/ICompressionCodec.h>
#include <Storages/ColumnCodec.h>
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

    CompressionSettings(CompressionMethod method_, ColumnCodecs codecs_)
        : method(method_)
        , codecs(codecs_)
    {
    }

    CompressionSettings(CompressionMethod method_, int level_)
        : method(method_)
        , level(level_)
    {
    }

    CompressionSettings(CompressionMethod method_, PipePtr codec_)
        : method(method_)
        , codec(codec_)
    {
    }

    CompressionSettings(CompressionMethod method_, int level_, PipePtr codec_)
            : method(method_)
            , level(level_)
            , codec(codec_)
    {
    }

    CompressionSettings(CompressionMethod method_, ColumnCodecs codecs_, const String & name)
        : method(method_)
        , codecs(codecs_)
        , codec (codecs[name])
    {
    }

    CompressionSettings(const Settings & settings);

    ColumnCodecs codecs;
    PipePtr codec;

    void setCodecs(ColumnCodecs _codecs)
    {
        codecs = _codecs;
    }

    CompressionSettings getNamedSettings(const String & name)
    {
        return CompressionSettings(method, codecs[name]);
    }

    static int getDefaultLevel(CompressionMethod method);
};

}
