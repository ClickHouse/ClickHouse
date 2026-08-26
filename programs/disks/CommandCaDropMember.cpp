#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasDecommission.h>
#include <Common/Exception.h>
#include <ICommand.h>

#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Operator-driven decommission of a DEAD pool member's namespaces, debris, staging, roots objects
/// and mount slot (`Cas::decommissionPoolMember`, design 2026-07-13-cas-pool-member-decommission
/// §core). Refuses a live member internally; this command only opens the CA disk read-only and
/// forwards the pool handle -- the admin claim itself happens inside `decommissionPoolMember`.
class CommandCaDropMember final : public ICommand
{
public:
    CommandCaDropMember() : ICommand("CommandCaDropMember")
    {
        command_name = "cas-drop-member";
        description = "Decommission a DEAD pool member: erase its namespaces, debris, staging, roots "
                      "objects and mount slot. Refuses a live member. Open the CA disk read-only "
                      "(the admin claim is made internally).";
        options_description.add_options()("member", po::value<String>(), "server_root_id of the dead member");
        positional_options_description.add("member", 1);
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        const String srid = getValueFromCommandLineOptionsThrow<String>(options, "member");
        auto disk = client.getCurrentDiskWithPath().getDisk();

        auto * dos = dynamic_cast<DiskObjectStorage *>(disk.get());
        if (!dos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cas-drop-member: '{}' is not an object-storage disk", disk->getName());

        auto * ca = dynamic_cast<ContentAddressedMetadataStorage *>(dos->getMetadataStorage().get());
        if (!ca)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cas-drop-member: disk '{}' is not content-addressed", disk->getName());

        if (!ca->isReadOnly())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "cas-drop-member: open the CA disk read-only (a writable open would claim this tool's "
                "own server_root_id; the decommission claim happens internally)");

        const auto host_store = ca->store();
        const auto report = Cas::decommissionPoolMember(
            host_store->poolBackendPtr(), host_store->poolConfig(), srid);

        std::cout << "server_root_id=" << report.srid << "\n"
                  << "namespaces_removed=" << report.namespaces_removed << "\n"
                  << "namespaces_already_removed=" << report.namespaces_already_removed << "\n"
                  << "committed_refs_removed=" << report.committed_refs_removed << "\n"
                  << "precommits_removed=" << report.precommits_removed << "\n"
                  << "manifest_debris_removed=" << report.manifest_debris_removed << "\n"
                  << "staging_objects_removed=" << report.staging_objects_removed << "\n"
                  << "mountpoint_objects_removed=" << report.mountpoint_objects_removed << "\n"
                  << "slot_removed=" << (report.slot_removed ? "true" : "false") << "\n";
        for (const auto & w : report.warnings)
            std::cout << "warning=" << w << "\n";
    }
};

CommandPtr makeCommandCaDropMember()
{
    return std::make_shared<DB::CommandCaDropMember>();
}

}
