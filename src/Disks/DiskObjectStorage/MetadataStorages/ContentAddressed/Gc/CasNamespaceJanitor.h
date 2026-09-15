#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <functional>
#include <vector>

namespace DB::Cas
{

struct NamespaceJanitorResult
{
    uint64_t pages = 0;
    uint64_t keys = 0;
    uint64_t deleted = 0;
    uint64_t leaked = 0;
    std::vector<String> anomalies;
};

/// Runs one bounded, leak-only page over the physical namespace ownership tree.
class NamespaceJanitor
{
public:
    NamespaceJanitor(CasRequests & requests_, const Layout & layout_, size_t page_budget_)
        : requests(requests_), layout(layout_), page_budget(page_budget_) {}

    /// `liveness` is admitted once for the whole page (one `CasOperation` covers the read, the list,
    /// every delete and the cursor publication): a fact the fence cannot see, such as "this tenure
    /// still holds the GC round's own lease" -- see `CasRequests::admit`. It is SAMPLED BEFORE EVERY
    /// REQUEST the page makes (and before every reissue of one), not just at the two points this
    /// function itself checks `op.admitted()` -- so it must be cheap and must never throw. A sample
    /// that returns false ends whichever request was about to be sent: a read verb (the maintenance
    /// read, the list, a HEAD) throws out of this call, and a write verb (a delete, the cursor
    /// publication) reports it as `GaveUp` rather than sending anything.
    NamespaceJanitorResult runOnePage(bool suppress_deletes, Liveness liveness);

private:
    CasRequests & requests;
    const Layout & layout;
    size_t page_budget;
};

}
