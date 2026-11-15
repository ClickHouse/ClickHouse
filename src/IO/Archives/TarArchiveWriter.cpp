#include <IO/Archives/TarArchiveWriter.h>

#if USE_LIBARCHIVE
namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int CANNOT_PACK_ARCHIVE;
}
void TarArchiveWriter::setCompression(const String & compression_method_, int compression_level_)
{
    // throw an error unless setCompression is passed the default value
    if (compression_method_.empty() && compression_level_ == -1)
        return;
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Using compression_method and compression_level options are not supported for tar archives");
}

void TarArchiveWriter::setFormatAndSettings()
{
    archive_write_set_format_pax_restricted(archive);
    inferCompressionFromPath();
}

void TarArchiveWriter::inferCompressionFromPath()
{
    if (path_to_archive.ends_with(".tar.gz") || path_to_archive.ends_with(".tgz"))
        archive_write_add_filter_gzip(archive);
    else if (path_to_archive.ends_with(".tar.bz2"))
        archive_write_add_filter_bzip2(archive);
    else if (path_to_archive.ends_with(".tar.lzma"))
        archive_write_add_filter_lzma(archive);
    else if (path_to_archive.ends_with(".tar.zst") || path_to_archive.ends_with(".tzst"))
        archive_write_add_filter_zstd(archive);
    else if (path_to_archive.ends_with(".tar.xz"))
        archive_write_add_filter_xz(archive);
    else if (!path_to_archive.ends_with(".tar"))
        throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Unknown compression format");
}
}
#endif
