#include <Common/config.h>
#include <Interpreters/Context.h>

#include "Disks/DiskFactory.h"
#include "DiskEncrypted.h"

#include <Functions/FileEncryption.h>
#include <IO/ReadEncryptedBuffer.h>
#include <IO/WriteEncryptedBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DISK_INDEX;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

using DiskEncryptedPtr = std::shared_ptr<DiskEncrypted>;
using namespace FileEncryption;

class DiskEncryptedReservation : public IReservation
{
public:
    DiskEncryptedReservation(DiskEncryptedPtr disk_, std::unique_ptr<IReservation> reservation_)
        : disk(std::move(disk_)), reservation(std::move(reservation_))
    {
    }

    UInt64 getSize() const override { return reservation->getSize(); }

    DiskPtr getDisk(size_t i) const override {
        if (i != 0)
            throw Exception("Can't use i != 0 with single disk reservation", ErrorCodes::INCORRECT_DISK_INDEX);
        return disk;
    }

    Disks getDisks() const override { return {disk}; }

    void update(UInt64 new_size) override { reservation->update(new_size); }

private:
    DiskEncryptedPtr disk;
    std::unique_ptr<IReservation> reservation;
};

ReservationPtr DiskEncrypted::reserve(UInt64 bytes)
{
    auto reservation = delegate->reserve(bytes);
    if (!reservation)
        return {};
    return std::make_unique<DiskEncryptedReservation>(std::static_pointer_cast<DiskEncrypted>(shared_from_this()), std::move(reservation));
}

std::unique_ptr<ReadBufferFromFileBase> DiskEncrypted::readFile(
    const String & path,
    size_t buf_size,
    size_t estimated_size,
    size_t aio_threshold,
    size_t mmap_threshold,
    MMappedFileCache * mmap_cache) const
{
    auto wrapped_path = wrappedPath(path);
    auto buffer = delegate->readFile(wrapped_path, buf_size, estimated_size, aio_threshold, mmap_threshold, mmap_cache);

    auto iv = GetRandomString(kIVSize);
    size_t offset = 0;

    if (exists(path) && getFileSize(path))
    {
        iv = ReadIV(kIVSize, *buffer);
        offset = kIVSize;
    }

    return std::make_unique<ReadEncryptedBuffer>(buf_size, std::move(buffer), iv, key, offset);
}

std::unique_ptr<WriteBufferFromFileBase> DiskEncrypted::writeFile(
    const String & path,
    size_t buf_size,
    WriteMode mode)
{
    auto wrapped_path = wrappedPath(path);
    auto iv = GetRandomString(kIVSize);

    if (exists(path) && getFileSize(path))
    {
        auto read_buffer = delegate->readFile(wrapped_path, kIVSize);
        iv = ReadIV(kIVSize, *read_buffer);
    }

    auto buffer = delegate->writeFile(wrapped_path, buf_size, mode);
    return std::make_unique<WriteEncryptedBuffer>(buf_size, std::move(buffer), iv, key,
                                                  mode == WriteMode::Append ? delegate->getFileSize(wrapped_path) : 0);
}


size_t DiskEncrypted::getFileSize(const String & path) const
{
    auto wrapped_path = wrappedPath(path);
    size_t size = delegate->getFileSize(wrapped_path);
    return size > kIVSize ? (size - kIVSize) : 0;
}

void DiskEncrypted::truncateFile(const String & path, size_t size)
{
    auto wrapped_path = wrappedPath(path);
    delegate->truncateFile(wrapped_path, size ? (size + kIVSize) : 0);
}

SyncGuardPtr DiskEncrypted::getDirectorySyncGuard(const String & path) const
{
    auto wrapped_path = wrappedPath(path);
    return delegate->getDirectorySyncGuard(wrapped_path);
}

void DiskEncrypted::applyNewSettings(
    const Poco::Util::AbstractConfiguration & config,
    ContextConstPtr /*context*/,
    const String & config_prefix,
    const DisksMap & map)
{
    String wrapped_disk_name = config.getString(config_prefix + ".disk", "");
    if (wrapped_disk_name.empty())
        throw Exception("The wrapped disk name can not be empty. An encrypted disk is a wrapper over another disk. "
                        "Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

    key = config.getString(config_prefix + ".key", "");
    if (key.empty())
        throw Exception("Encrypted disk key can not be empty. Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

    auto wrapped_disk = map.find(wrapped_disk_name);
    if (wrapped_disk == map.end())
        throw Exception("The wrapped disk must have been announced earlier. No disk with name " + wrapped_disk_name + ". Disk " + name,
                        ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
    delegate = wrapped_disk->second;

    disk_path = config.getString(config_prefix + ".path", "");
    initialize();
}

void registerDiskEncrypted(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextConstPtr /*context*/,
		      const DisksMap & map) -> DiskPtr {

        String wrapped_disk_name = config.getString(config_prefix + ".disk", "");
        if (wrapped_disk_name.empty())
            throw Exception("The wrapped disk name can not be empty. An encrypted disk is a wrapper over another disk. "
                            "Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

        String key = config.getString(config_prefix + ".key", "");
        if (key.empty())
            throw Exception("Encrypted disk key can not be empty. Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
	if (key.size() != CipherKeyLength(DefaultCipher()))
            throw Exception("Expected key with size " + std::to_string(CipherKeyLength(DefaultCipher())) + ", got key with size " + std::to_string(key.size()),
			    ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

        auto wrapped_disk = map.find(wrapped_disk_name);
        if (wrapped_disk == map.end())
            throw Exception("The wrapped disk must have been announced earlier. No disk with name " + wrapped_disk_name + ". Disk " + name,
                            ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

        String relative_path = config.getString(config_prefix + ".path", "");

        return std::make_shared<DiskEncrypted>(name, wrapped_disk->second, key, relative_path);
    };
    factory.registerDiskType("encrypted", creator);
}

}
