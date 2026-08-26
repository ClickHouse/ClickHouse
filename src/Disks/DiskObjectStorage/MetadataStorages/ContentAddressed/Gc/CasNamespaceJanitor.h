#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
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
    NamespaceJanitor(Backend & backend_, const Layout & layout_, size_t page_budget_)
        : backend(backend_), layout(layout_), page_budget(page_budget_) {}

    NamespaceJanitorResult runOnePage(bool suppress_deletes, const std::function<bool()> & fence_held);

private:
    Backend & backend;
    const Layout & layout;
    size_t page_budget;
};

}
