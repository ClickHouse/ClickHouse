#include "DiskDecorator.h"
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>

namespace DB
{
DiskDecorator::DiskDecorator(const DiskPtr & delegate_) : delegate(delegate_)
{
}

const String & DiskDecorator::getName() const
{
    return delegate->getName();
}

ReservationPtr DiskDecorator::reserve(UInt64 bytes)
{
    return delegate->reserve(bytes);
}

const String & DiskDecorator::getPath() const
{
    return delegate->getPath();
}

UInt64 DiskDecorator::getTotalSpace() const
{
    return delegate->getTotalSpace();
}

UInt64 DiskDecorator::getAvailableSpace() const
{
    return delegate->getAvailableSpace();
}

UInt64 DiskDecorator::getUnreservedSpace() const
{
    return delegate->getUnreservedSpace();
}

UInt64 DiskDecorator::getKeepingFreeSpace() const
{
    return delegate->getKeepingFreeSpace();
}

bool DiskDecorator::exists(const String & path) const
{
    return delegate->exists(path);
}

bool DiskDecorator::isFile(const String & path) const
{
    return delegate->isFile(path);
}

bool DiskDecorator::isDirectory(const String & path) const
{
    return delegate->isDirectory(path);
}

size_t DiskDecorator::getFileSize(const String & path) const
{
    return delegate->getFileSize(path);
}

void DiskDecorator::createDirectory(const String & path)
{
    delegate->createDirectory(path);
}

void DiskDecorator::createDirectories(const String & path)
{
    delegate->createDirectories(path);
}

void DiskDecorator::clearDirectory(const String & path)
{
    delegate->clearDirectory(path);
}

void DiskDecorator::moveDirectory(const String & from_path, const String & to_path)
{
    delegate->moveDirectory(from_path, to_path);
}

DiskDirectoryIteratorPtr DiskDecorator::iterateDirectory(const String & path)
{
    return delegate->iterateDirectory(path);
}

void DiskDecorator::createFile(const String & path)
{
    delegate->createFile(path);
}

void DiskDecorator::moveFile(const String & from_path, const String & to_path)
{
    delegate->moveFile(from_path, to_path);
}

void DiskDecorator::replaceFile(const String & from_path, const String & to_path)
{
    delegate->replaceFile(from_path, to_path);
}

void DiskDecorator::copyFile(const String & from_path, const String & to_path)
{
    delegate->copyFile(from_path, to_path);
}

void DiskDecorator::copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path)
{
    delegate->copy(from_path, to_disk, to_path);
}

void DiskDecorator::listFiles(const String & path, std::vector<String> & file_names)
{
    delegate->listFiles(path, file_names);
}

std::unique_ptr<ReadBufferFromFileBase>
DiskDecorator::readFile(const String & path, size_t buf_size, size_t estimated_size, size_t aio_threshold, size_t mmap_threshold) const
{
    return delegate->readFile(path, buf_size, estimated_size, aio_threshold, mmap_threshold);
}

std::unique_ptr<WriteBufferFromFileBase>
DiskDecorator::writeFile(const String & path, size_t buf_size, WriteMode mode, size_t estimated_size, size_t aio_threshold)
{
    return delegate->writeFile(path, buf_size, mode, estimated_size, aio_threshold);
}

void DiskDecorator::remove(const String & path)
{
    delegate->remove(path);
}

void DiskDecorator::removeRecursive(const String & path)
{
    delegate->removeRecursive(path);
}

void DiskDecorator::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    delegate->setLastModified(path, timestamp);
}

Poco::Timestamp DiskDecorator::getLastModified(const String & path)
{
    return delegate->getLastModified(path);
}

void DiskDecorator::setReadOnly(const String & path)
{
    delegate->setReadOnly(path);
}

void DiskDecorator::createHardLink(const String & src_path, const String & dst_path)
{
    delegate->createHardLink(src_path, dst_path);
}

void DiskDecorator::truncateFile(const String & path, size_t size)
{
    delegate->truncateFile(path, size);
}

int DiskDecorator::open(const String & path, mode_t mode) const
{
    return delegate->open(path, mode);
}

void DiskDecorator::close(int fd) const
{
    delegate->close(fd);
}

void DiskDecorator::sync(int fd) const
{
    delegate->sync(fd);
}

Executor & DiskDecorator::getExecutor()
{
    return delegate->getExecutor();
}

}
