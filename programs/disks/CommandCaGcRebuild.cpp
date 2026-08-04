#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>
#include <ICommand.h>

#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// The gc/state disaster-recovery command (spec 2026-07-03): recomputes the in-degree baseline from
/// raw owner state and CASes a fresh gc/state when the guard has refused every regular round (a lost
/// gc/state over trimmed journal history — see docs/superpowers/cas/04-gc-protocol.md#gc-rebuild).
///
/// REQUIRES a read-only-opened disk, same as fsck/cas-gc-dryrun: this tool must never claim the live
/// server's mount (a second live mounter racing the real GC's lease/writes is exactly the split-brain
/// class the protocol is designed to prevent). Unlike fsck/cas-gc-dryrun, rebuildBaseline DOES write
/// (a single gc/state CAS) — that write is a deliberate, explicit, operator-invoked exception to
/// "read-only means no writes", gated on the SAME `isReadOnly()` check so it can only run against a
/// disk configured with <readonly>true</readonly> (i.e. never against the disk a live server has
/// mounted for read-write traffic).
class CommandCaGcRebuild final : public ICommand
{
public:
    CommandCaGcRebuild() : ICommand("CommandCaGcRebuild")
    {
        command_name = "cas-gc-rebuild";
        description = "Disaster recovery: rebuild a content-addressed pool's gc/state baseline from raw owner "
                      "state after the GC guard has refused every round (see CORRUPTED_DATA in the gc log). "
                      "Requires a read-only-opened disk; never run against a disk a live server has mounted.";
        options_description.add_options()("force", "bypass the \"healthy state\" refusal (rebuild even though gc/state and every referenced artifact look fine)");
    }

    void executeImpl(const CommandLineOptions & options, DisksClient & client) override
    {
        const bool force = options.contains("force");
        auto disk = client.getCurrentDiskWithPath().getDisk();

        auto * dos = dynamic_cast<DiskObjectStorage *>(disk.get());
        if (!dos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cas-gc-rebuild: '{}' is not an object-storage disk", disk->getName());

        auto * ca = dynamic_cast<ContentAddressedMetadataStorage *>(dos->getMetadataStorage().get());
        if (!ca)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cas-gc-rebuild: disk '{}' is not content-addressed", disk->getName());

        if (!ca->isReadOnly())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "cas-gc-rebuild: open the CA disk read-only (<readonly>true</readonly>) — this tool must never "
                "claim the live server's mount");

        /// gc_id uniqueness across instances is the Gc caller obligation (a random u128 per invocation);
        /// this is a one-shot command, so a fresh mint per run is exactly right (no stable-instance
        /// requirement here — rebuildBaseline does its own lease acquire/steal check internally).
        const UInt128 gc_id = (static_cast<UInt128>(thread_local_rng()) << 64) | thread_local_rng();
        Cas::Gc gc(ca->store(), gc_id);
        const Cas::RebuildReport rep = gc.rebuildBaseline(force);

        std::cout << "performed=" << (rep.performed ? 1 : 0) << " round=" << rep.round << " generation=" << rep.generation
                  << " namespaces=" << rep.namespaces << " shards=" << rep.shards << " committed_refs=" << rep.committed_refs
                  << " live_precommits=" << rep.live_precommits << " unowned_alive_manifests=" << rep.unowned_alive_manifests
                  << " edges=" << rep.edges << " clamped_shards=" << rep.clamped_shards << "\n";

        if (!rep.performed)
        {
            std::cout << "refusal=" << rep.refusal << "\n";
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cas-gc-rebuild: refused: {}", rep.refusal);
        }
    }
};

CommandPtr makeCommandCaGcRebuild()
{
    return std::make_shared<DB::CommandCaGcRebuild>();
}

}
