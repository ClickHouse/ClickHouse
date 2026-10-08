#pragma once

#include "config.h"

#if USE_LIBARCHIVE

#    include <IO/Archives/LibArchiveWriter.h>
namespace DB
{
using namespace std::literals;

class TarArchiveWriter : public LibArchiveWriter
{
public:
    explicit TarArchiveWriter(
        const String & path_to_archive_,
        std::unique_ptr<WriteBuffer> archive_write_buffer_,
        size_t buf_size_,
        size_t adaptive_buffer_max_size_)
        : LibArchiveWriter(path_to_archive_, std::move(archive_write_buffer_), buf_size_, adaptive_buffer_max_size_)
    {
        createArchive();
    }

    void setCompression(const String & compression_method_, int compression_level_) override;
    void setFormatAndSettings() override;
    void inferCompressionFromPath();
};
}
#endif
