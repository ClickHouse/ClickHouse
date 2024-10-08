#include "PartMetadataManagerOrdinary.h"

#include <IO/ReadBufferFromFileBase.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

std::unique_ptr<ReadBuffer> PartMetadataManagerOrdinary::read(const String & file_name) const
{
    size_t file_size = part->getDataPartStorage().getFileSize(file_name);
    auto read_settings = getReadSettings().adjustBufferSize(file_size);
    /// Default read method is pread_threadpool, but there is not much point in it here.
    read_settings.local_fs_method = LocalFSReadMethod::pread;

    auto res = part->getDataPartStorage().readFile(file_name, read_settings, file_size, std::nullopt);

    if (isCompressedFromFileName(file_name))
        return std::make_unique<CompressedReadBufferFromFile>(std::move(res));

    return res;
}

bool PartMetadataManagerOrdinary::exists(const String & file_name) const
{
    return part->getDataPartStorage().exists(file_name);
}


}
