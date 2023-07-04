#pragma once

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

class IAsynchronousReader;

enum class FilesystemReaderType
{
    SYNCHRONOUS_LOCAL_FS_READER,
    ASYNCHRONOUS_LOCAL_FS_READER,
    ASYNCHRONOUS_REMOTE_FS_READER,
};

IAsynchronousReader & getThreadPoolReader(FilesystemReaderType type);

std::unique_ptr<IAsynchronousReader> createThreadPoolReader(
    FilesystemReaderType type,
    const Poco::Util::AbstractConfiguration & config);

}
