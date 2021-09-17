#pragma once

#include <condition_variable>

namespace DB
{

enum class FileDownloadStatus
{
    NONE,
    DOWNLOADING,
    DOWNLOADED,
    ERROR
};

struct FileDownloadMetadata
{
    /// Thread waits on this condition if download process is in progress.
    std::condition_variable condition;
    FileDownloadStatus status = FileDownloadStatus::NONE;
};

}
