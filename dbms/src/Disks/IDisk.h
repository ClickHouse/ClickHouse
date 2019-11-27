#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Core/Types.h>

#include <memory>
#include <utility>
#include <boost/noncopyable.hpp>


namespace CurrentMetrics
{
extern const Metric DiskSpaceReservedForMerge;
}

namespace DB
{
class IDiskFile;
using DiskFilePtr = std::shared_ptr<IDiskFile>;

class DiskDirectoryIterator;
class IDiskDirectoryIteratorImpl;
using DiskDirectoryIteratorImplPtr = std::unique_ptr<IDiskDirectoryIteratorImpl>;

class IReservation;
using ReservationPtr = std::unique_ptr<IReservation>;

class ReadBuffer;
class WriteBuffer;

/**
 * Provide interface for reservation.
 */
class Space : public std::enable_shared_from_this<Space>
{
public:
    virtual const String & getName() const = 0;

    virtual ReservationPtr reserve(UInt64 bytes) const = 0;

    virtual ~Space() = default;
};

using SpacePtr = std::shared_ptr<const Space>;

class IDisk : public Space
{
public:
    virtual const String & getPath() const = 0;

    /// Total available space on disk.
    virtual UInt64 getTotalSpace() const = 0;

    /// Space currently available on disk.
    virtual UInt64 getAvailableSpace() const = 0;

    /// Currently available (prev method) minus already reserved space.
    virtual UInt64 getUnreservedSpace() const = 0;

    /// Amount of bytes which should be kept free on this disk.
    virtual UInt64 getKeepingFreeSpace() const { return 0; }

    virtual DiskFilePtr file(const String & path) const = 0;
};

using DiskPtr = std::shared_ptr<const IDisk>;
using Disks = std::vector<DiskPtr>;

class IDiskFile : public std::enable_shared_from_this<IDiskFile>
{
public:
    friend class DiskDirectoryIterator;

    /// Return disk which the file belongs to.
    const DiskPtr & disk() const { return disk_ptr; }

    const String & path() const { return rel_path; }

    String fullPath() const { return disk_ptr->getPath() + rel_path; }

    /// Returns true if the file exists.
    virtual bool exists() const = 0;

    /// Returns true if the file is a directory.
    virtual bool isDirectory() const = 0;

    /// Creates a directory.
    virtual void createDirectory() = 0;

    /// Creates a directory and all parent directories if necessary.
    virtual void createDirectories() = 0;

    virtual void moveTo(const String & new_path) = 0;

    virtual void copyTo(const String & new_path) = 0;

    /// Open the file for read and returns ReadBuffer object.
    virtual std::unique_ptr<ReadBuffer> read() const = 0;

    /// Open the file for write and returns WriteBuffer object.
    virtual std::unique_ptr<WriteBuffer> write() = 0;

    virtual ~IDiskFile() = default;

protected:
    IDiskFile(const DiskPtr & disk_ptr_, const String & rel_path_) : disk_ptr(disk_ptr_), rel_path(rel_path_) {}

private:
    virtual DiskDirectoryIteratorImplPtr iterateDirectory() = 0;

protected:
    DiskPtr disk_ptr;
    String rel_path;
};

class IDiskDirectoryIteratorImpl
{
public:
    virtual const String & name() const = 0;

    virtual const DiskFilePtr & get() const = 0;

    virtual void next() = 0;

    virtual bool isValid() const = 0;

    virtual ~IDiskDirectoryIteratorImpl() = default;
};

class DiskDirectoryIterator final
{
public:
    DiskDirectoryIterator() = default;

    explicit DiskDirectoryIterator(const DiskFilePtr & file) : impl(file->iterateDirectory()) {}

    String name() const { return impl->name(); }

    DiskDirectoryIterator & operator++()
    {
        impl->next();
        return *this;
    }

    const DiskFilePtr & operator*() const { return impl->get(); }
    const DiskFilePtr & operator->() const { return impl->get(); }

    bool operator==(const DiskDirectoryIterator & iterator) const
    {
        if (this == &iterator)
            return true;

        if (iterator.impl && iterator.impl->isValid())
            return false;

        if (impl && impl->isValid())
            return false;

        return true;
    }

    bool operator!=(const DiskDirectoryIterator & iterator) const { return !operator==(iterator); }

private:
    DiskDirectoryIteratorImplPtr impl;
};


/**
 * Information about reserved size on particular disk.
 */
class IReservation
{
public:
    /// Get reservation size.
    UInt64 getSize() const { return size; }

    /// Get disk where reservation take place.
    const DiskPtr & getDisk() const { return disk_ptr; }

    /// Changes amount of reserved space.
    virtual void update(UInt64 new_size) = 0;

    /// Unreserves reserved space.
    virtual ~IReservation() = default;

protected:
    explicit IReservation(const DiskPtr & disk_ptr_, UInt64 size_)
        : disk_ptr(disk_ptr_), size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {
    }

protected:
    DiskPtr disk_ptr;
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
};

}
