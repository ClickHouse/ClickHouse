#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcReadAhead.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasKeyReader.h>

namespace DB::Cas
{

/// The reader over the GC's read-ahead: a hint is a worker request, a take is the worker's result
/// (or an inline read for a key nobody hinted), and a discard drops a hinted key and counts it as
/// wasted at once.
class ReadAheadKeyReader final : public KeyReader
{
public:
    explicit ReadAheadKeyReader(GcReadAhead & reads_) : reads(reads_) {}
    void hint(const String & key) override { reads.hintRead(key); }
    std::optional<Object> take(const String & key) override { return reads.takeRead(key); }
    void discard(const String & key) override { reads.discardRead(key); }
    size_t pending() const override { return reads.pending(); }
    size_t window() const override { return reads.window(); }

private:
    GcReadAhead & reads;
};

}
