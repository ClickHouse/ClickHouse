#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Common/Exception.h>
#include <ICommand.h>

#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CommandCaGcDryRun final : public ICommand
{
public:
    CommandCaGcDryRun() : ICommand("CommandCaGcDryRun")
    {
        command_name = "cas-gc-dryrun";
        description = "Preview the next GC round's deletes for a content-addressed pool (read-only, no deletes).";
    }

    void executeImpl(const CommandLineOptions &, DisksClient & client) override
    {
        auto disk = client.getCurrentDiskWithPath().getDisk();

        auto * dos = dynamic_cast<DiskObjectStorage *>(disk.get());
        if (!dos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cas-gc-dryrun: '{}' is not an object-storage disk", disk->getName());

        auto * ca = dynamic_cast<ContentAddressedMetadataStorage *>(dos->getMetadataStorage().get());
        if (!ca)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cas-gc-dryrun: disk '{}' is not content-addressed", disk->getName());

        if (!ca->isReadOnly())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cas-gc-dryrun: open the CA disk read-only");

        /// A non-leader, read-only Gc handle: previewDeletes never acquires the lease or writes.
        Cas::Gc gc(ca->store(), UInt128(1));
        const auto preview = gc.previewDeletes();

        std::cout << "preview_deletes=" << preview.size() << "\n";
        for (const auto & p : preview)
            std::cout << p.reason << "\t" << p.key << "\t" << p.size << "\n";
    }
};

CommandPtr makeCommandCaGcDryRun()
{
    return std::make_shared<DB::CommandCaGcDryRun>();
}

}
