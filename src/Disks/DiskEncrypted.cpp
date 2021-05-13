#include <Common/config.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>

#include "Disks/DiskFactory.h"
#include "DiskEncrypted.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DISK_INDEX;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

using DiskEncryptedPtr = std::shared_ptr<DiskEncrypted>;

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
    auto reservation = wrapped_disk->reserve(bytes);
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
    MMappedFileCache * mmap_cache) const {
    return wrapped_disk->readFile(path, buf_size, estimated_size, aio_threshold, mmap_threshold, mmap_cache);
}

std::unique_ptr<WriteBufferFromFileBase> DiskEncrypted::writeFile(
    const String & path,
    size_t buf_size,
    WriteMode mode) {
    return wrapped_disk->writeFile(path, buf_size, mode);
}

void DiskEncrypted::truncateFile(const String & path, size_t size) {
    wrapped_disk->truncateFile(path, size);
}

SyncGuardPtr DiskEncrypted::getDirectorySyncGuard(const String & path) const {
    return wrapped_disk->getDirectorySyncGuard(path);
}

void registerDiskEncrypted(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextConstPtr context) -> DiskPtr {
        String wrapped_disk_name = config.getString(config_prefix + ".disk", "");
        if (wrapped_disk_name.empty())
            throw Exception("The wrapped disk name can not be empty. An encrypted disk is a wrapper over another disk. "
                            "Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

        String key = config.getString(config_prefix + ".key", "");
        if (key.empty())
            throw Exception("Encrypted disk key can not be empty. Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

        const auto& disks = context->getDisksMap();
        const auto& wrapped_disk = disks.find(wrapped_disk_name);
        if (wrapped_disk == disks.end())
            throw Exception("The wrapped disk must have been announced earlier. No disk with name " + wrapped_disk_name + ". Disk " + name,
                            ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

        return std::make_shared<DiskEncrypted>(name, wrapped_disk->second, key);
    };
    factory.registerDiskType("encrypted", creator);
}

}
