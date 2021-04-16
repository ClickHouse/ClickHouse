#include "DiskRestartProxy.h"

namespace DB
{

DiskRestartProxy::DiskRestartProxy(DiskPtr & delegate_)
    : DiskDecorator(delegate_) { }

ReservationPtr DiskRestartProxy::reserve(UInt64 bytes)
{
    ReadLock lock (mutex);
    return DiskDecorator::reserve(bytes);
}

const String & DiskRestartProxy::getPath() const
{
    ReadLock lock (mutex);
    return DiskDecorator::getPath();
}

UInt64 DiskRestartProxy::getTotalSpace() const
{
    ReadLock lock (mutex);
    return DiskDecorator::getTotalSpace();
}
UInt64 DiskRestartProxy::getAvailableSpace() const
{
    ReadLock lock (mutex);
    return DiskDecorator::getAvailableSpace();
}
UInt64 DiskRestartProxy::getUnreservedSpace() const
{
    ReadLock lock (mutex);
    return DiskDecorator::getUnreservedSpace();
}
UInt64 DiskRestartProxy::getKeepingFreeSpace() const
{
    ReadLock lock (mutex);
    return DiskDecorator::getKeepingFreeSpace();
}
bool DiskRestartProxy::exists(const String & path) const
{
    ReadLock lock (mutex);
    return DiskDecorator::exists(path);
}
bool DiskRestartProxy::isFile(const String & path) const
{
    ReadLock lock (mutex);
    return DiskDecorator::isFile(path);
}
bool DiskRestartProxy::isDirectory(const String & path) const
{
    ReadLock lock (mutex);
    return DiskDecorator::isDirectory(path);
}
size_t DiskRestartProxy::getFileSize(const String & path) const
{
    ReadLock lock (mutex);
    return DiskDecorator::getFileSize(path);
}
void DiskRestartProxy::createDirectory(const String & path)
{
    ReadLock lock (mutex);
    DiskDecorator::createDirectory(path);
}
void DiskRestartProxy::createDirectories(const String & path)
{
    ReadLock lock (mutex);
    DiskDecorator::createDirectories(path);
}
void DiskRestartProxy::clearDirectory(const String & path)
{
    ReadLock lock (mutex);
    DiskDecorator::clearDirectory(path);
}
void DiskRestartProxy::moveDirectory(const String & from_path, const String & to_path)
{
    ReadLock lock (mutex);
    DiskDecorator::moveDirectory(from_path, to_path);
}

DiskDirectoryIteratorPtr DiskRestartProxy::iterateDirectory(const String & path)
{
    ReadLock lock (mutex);
    return DiskDecorator::iterateDirectory(path);
}

void DiskRestartProxy::createFile(const String & path)
{
    ReadLock lock (mutex);
    DiskDecorator::createFile(path);
}

void DiskRestartProxy::moveFile(const String & from_path, const String & to_path)
{
    ReadLock lock (mutex);
    DiskDecorator::moveFile(from_path, to_path);
}

void DiskRestartProxy::replaceFile(const String & from_path, const String & to_path)
{
    ReadLock lock (mutex);
    DiskDecorator::replaceFile(from_path, to_path);
}

void DiskRestartProxy::copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path)
{
    ReadLock lock (mutex);
    DiskDecorator::copy(from_path, to_disk, to_path);
}

void DiskRestartProxy::listFiles(const String & path, std::vector<String> & file_names)
{
    ReadLock lock (mutex);
    DiskDecorator::listFiles(path, file_names);
}

std::unique_ptr<ReadBufferFromFileBase> DiskRestartProxy::readFile(
    const String & path, size_t buf_size, size_t estimated_size, size_t aio_threshold, size_t mmap_threshold, MMappedFileCache * mmap_cache)
    const
{
    ReadLock lock (mutex);
    return DiskDecorator::readFile(path, buf_size, estimated_size, aio_threshold, mmap_threshold, mmap_cache);
}

std::unique_ptr<WriteBufferFromFileBase> DiskRestartProxy::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    ReadLock lock (mutex);
    return DiskDecorator::writeFile(path, buf_size, mode);
}

void DiskRestartProxy::removeFile(const String & path)
{
    ReadLock lock (mutex);
    DiskDecorator::removeFile(path);
}

void DiskRestartProxy::removeFileIfExists(const String & path)
{
    ReadLock lock (mutex);
    DiskDecorator::removeFileIfExists(path);
}

void DiskRestartProxy::removeDirectory(const String & path)
{
    ReadLock lock (mutex);
    DiskDecorator::removeDirectory(path);
}

void DiskRestartProxy::removeRecursive(const String & path)
{
    ReadLock lock (mutex);
    DiskDecorator::removeRecursive(path);
}

void DiskRestartProxy::removeSharedFile(const String & path, bool keep_s3)
{
    ReadLock lock (mutex);
    DiskDecorator::removeSharedFile(path, keep_s3);
}

void DiskRestartProxy::removeSharedRecursive(const String & path, bool keep_s3)
{
    ReadLock lock (mutex);
    DiskDecorator::removeSharedRecursive(path, keep_s3);
}

void DiskRestartProxy::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    ReadLock lock (mutex);
    DiskDecorator::setLastModified(path, timestamp);
}

Poco::Timestamp DiskRestartProxy::getLastModified(const String & path)
{
    ReadLock lock (mutex);
    return DiskDecorator::getLastModified(path);
}

void DiskRestartProxy::setReadOnly(const String & path)
{
    ReadLock lock (mutex);
    DiskDecorator::setReadOnly(path);
}

void DiskRestartProxy::createHardLink(const String & src_path, const String & dst_path)
{
    ReadLock lock (mutex);
    DiskDecorator::createHardLink(src_path, dst_path);
}

void DiskRestartProxy::truncateFile(const String & path, size_t size)
{
    ReadLock lock (mutex);
    DiskDecorator::truncateFile(path, size);
}

String DiskRestartProxy::getUniqueId(const String & path) const
{
    ReadLock lock (mutex);
    return DiskDecorator::getUniqueId(path);
}

bool DiskRestartProxy::checkUniqueId(const String & id) const
{
    ReadLock lock (mutex);
    return DiskDecorator::checkUniqueId(id);
}

void DiskRestartProxy::restart(ContextConstPtr context)
{
    /// Speed up processing unhealthy requests.
    DiskDecorator::shutdown();

    WriteLock lock (mutex, std::defer_lock);

    LOG_INFO(log, "Acquiring lock to restart disk {}", DiskDecorator::getName());

    lock.lock();

    LOG_INFO(log, "Restart disk {}", DiskDecorator::getName());
}

}
