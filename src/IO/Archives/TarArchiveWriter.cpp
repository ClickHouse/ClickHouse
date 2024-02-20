#include <IO/Archives/TarArchiveWriter.h>

#if USE_LIBARCHIVE
namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_PACK_ARCHIVE;
extern const int NOT_IMPLEMENTED;
}
void TarArchiveWriter::setCompression(const String & compression_method_, int compression_level_)
{
    // throw an error unless setCompression is passed the default value
    if (compression_method_.empty() && compression_level_ == -1)
        return;
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Using compression_method and compression_level options are not supported for tar archives");
}

void TarArchiveWriter::setFormatAndSettings(Archive archive_)
{
    archive_write_set_format_pax_restricted(archive_);
    inferCompressionFromPath();
}

void TarArchiveWriter::inferCompressionFromPath()
{
    if (path_to_archive.ends_with(".gz"))
        archive_write_add_filter_gzip(archive);
    else if (path_to_archive.ends_with(".bz2"))
        archive_write_add_filter_bzip2(archive);
    else if (path_to_archive.ends_with(".lzma"))
        archive_write_add_filter_lzma(archive);
    //else path ends in .tar and we dont do any compression
}
}
#endif
