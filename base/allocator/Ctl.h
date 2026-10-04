#pragma once

/// The `mallctl` namespace (jemalloc: `ctl.h`, `ctl.c`).
///
/// The namespace is a compile-time tree of `CtlNode`s (CtlTree.cpp) with exactly the children of jemalloc in exactly
/// the same order, so that MIBs (which are positional indices into the children arrays, or the numeric index at
/// indexed levels) are identical. Leaves are plain functions with the signature of jemalloc's `*_ctl` functions;
/// they are declared in CtlImpl.h and implemented per subtree (Ctl.cpp, CtlConfigOpt.cpp, CtlArenas.cpp, ...).
///
/// The C ABI (`je_mallctl`, `je_mallctlnametomib`, `je_mallctlbymib`) lives in Api.cpp: it checks `malloc_init`
/// (`EAGAIN` on failure), fetches the tsd and calls the functions below.

#include <allocator/Common.h>

namespace jemalloc
{

class ThreadState;

/// Maximum ctl tree depth. jemalloc: CTL_MAX_DEPTH
inline constexpr size_t CTL_MAX_DEPTH = 7;
/// jemalloc: CTL_MULTI_SETTING_MAX_LEN
inline constexpr size_t CTL_MULTI_SETTING_MAX_LEN = 1000;

/// Use as arena index in `arena.<i>.{purge,decay,dss}` and `stats.arenas.<i>.*`.
/// jemalloc: MALLCTL_ARENAS_ALL
inline constexpr unsigned MALLCTL_ARENAS_ALL = 4096;
/// Use as arena index in `stats.arenas.<i>.*` to access destroyed arenas.
/// jemalloc: MALLCTL_ARENAS_DESTROYED
inline constexpr unsigned MALLCTL_ARENAS_DESTROYED = 4097;

/// The function type of a leaf (jemalloc: the `ctl` member of `ctl_named_node_t`, the `*_ctl` functions).
/// Returns 0 or an errno value.
using CtlLeaf = int(ThreadState & tsd, const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen);
using CtlLeafFn = CtlLeaf *;

/// Checks the index `i` of an indexed level (jemalloc: the `index` member of `ctl_indexed_node_t`, the `*_index`
/// functions, which return either their "super" node or NULL). Returns true if the index is valid.
using CtlIndex = bool(ThreadState * tsdn, const size_t * mib, size_t miblen, size_t i);
using CtlIndexFn = CtlIndex *;

/// A node of the tree (jemalloc: `ctl_named_node_t`, `ctl_indexed_node_t`).
///
/// - A leaf (terminal node) has `leaf != nullptr` and no children.
/// - An inner node with named children has `children[0 .. nchildren)` and `index == nullptr`.
/// - An inner node with an indexed level has `index != nullptr` and `nchildren == 1`; `children` points to the
///   per-index node (jemalloc's "super" node with an empty name), whose children are the named children of `<i>`.
struct CtlNode
{
    const char * name;
    const CtlNode * children;
    size_t nchildren;
    CtlIndexFn index;
    CtlLeafFn leaf;

    constexpr bool isLeaf() const { return leaf != nullptr; }
    constexpr bool isIndexed() const { return index != nullptr; }
};

/// The root of the tree (jemalloc: `super_root_node`).
extern const CtlNode ctl_super_root_node[1];

/// jemalloc: ctl_byname
int ctlByName(ThreadState & tsd, const char * name, void * oldp, size_t * oldlenp, void * newp, size_t newlen);

/// Partial names succeed (the MIB of the inner node is returned); a too-small `*miblenp` returns a truncated MIB.
/// jemalloc: ctl_nametomib
int ctlNameToMib(ThreadState & tsd, const char * name, size_t * mibp, size_t * miblenp);

/// jemalloc: ctl_bymib
int ctlByMib(ThreadState & tsd, const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen);

/// Resolves `name` relative to the inner node `mib[0 .. miblen)`, writing the result to `mib + miblen`;
/// `*miblenp` is the capacity of `mib` on input and the total length on output.
/// jemalloc: ctl_mibnametomib
int ctlMibNameToMib(ThreadState & tsd, size_t * mib, size_t miblen, const char * name, size_t * miblenp);

/// jemalloc: ctl_bymibname
int ctlByMibName(
    ThreadState & tsd,
    size_t * mib,
    size_t miblen,
    const char * name,
    size_t * miblenp,
    void * oldp,
    size_t * oldlenp,
    void * newp,
    size_t newlen);

/// Returns true on error. jemalloc: ctl_boot
bool ctlBoot();
/// jemalloc: ctl_prefork
void ctlPrefork(ThreadState * tsdn);
/// jemalloc: ctl_postfork_parent
void ctlPostforkParent(ThreadState * tsdn);
/// jemalloc: ctl_postfork_child
void ctlPostforkChild(ThreadState * tsdn);
/// jemalloc: ctl_mtx_assert_held
void ctlMtxAssertHeld(ThreadState * tsdn);

}
