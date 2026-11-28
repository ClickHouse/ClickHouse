#pragma once

/// The `mallctl` namespace (jemalloc: `ctl.h`, `ctl.c`).
///
/// The namespace is a compile-time tree of `MallctlNode`s (MallctlTree.cpp) with exactly the children of jemalloc in exactly
/// the same order, so that numeric paths (which are positional indices into the children arrays, or the numeric index at
/// indexed levels) are identical. Leaves are plain functions with the signature of jemalloc's `*_ctl` functions;
/// they are declared in MallctlImpl.h and implemented per subtree (Mallctl.cpp, CtlConfigOpt.cpp, MallctlArenas.cpp, ...).
///
/// The C ABI (`je_mallctl`, `je_mallctl_name_to_numeric_path`, `je_mallctl_by_numeric_path`) lives in API.cpp: it checks `malloc_init`
/// (`EAGAIN` on failure), fetches the tsd and calls the functions below.

#include <allocator/Common.h>

namespace jemalloc
{

class ThreadState;

/// Maximum ctl tree depth. jemalloc: CTL_MAX_DEPTH
inline constexpr size_t MALLCTL_MAX_DEPTH = 7;
/// jemalloc: CTL_MULTI_SETTING_MAX_LEN
inline constexpr size_t MALLCTL_MULTI_SETTING_MAX_LEN = 1000;

/// Use as arena index in `arena.<i>.{purge,decay,sbrk}` and `stats.arenas.<i>.*`.
/// jemalloc: MALLCTL_ARENAS_ALL
inline constexpr unsigned MALLCTL_ARENAS_ALL = 4096;
/// Use as arena index in `stats.arenas.<i>.*` to access destroyed arenas.
/// jemalloc: MALLCTL_ARENAS_DESTROYED
inline constexpr unsigned MALLCTL_ARENAS_DESTROYED = 4097;

/// The function type of a leaf (jemalloc: the `ctl` member of `ctl_named_node_t`, the `*_ctl` functions).
/// Returns 0 or an errno value.
using MallctlLeaf = int(
    ThreadState & thread_state,
    const size_t * numeric_path,
    size_t numeric_path_length,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length);
using MallctlLeafFunction = MallctlLeaf *;

/// Checks the index `i` of an indexed level (jemalloc: the `index` member of `ctl_indexed_node_t`, the `*_index`
/// functions, which return either their "super" node or NULL). Returns true if the index is valid.
using MallctlIndex = bool(ThreadState * thread_state, const size_t * numeric_path, size_t numeric_path_length, size_t i);
using MallctlIndexFunction = MallctlIndex *;

/// A node of the tree (jemalloc: `ctl_named_node_t`, `ctl_indexed_node_t`).
///
/// - A leaf (terminal node) has `leaf != nullptr` and no children.
/// - An inner node with named children has `children[0 .. num_children)` and `index == nullptr`.
/// - An inner node with an indexed level has `index != nullptr` and `num_children == 1`; `children` points to the
///   per-index node (jemalloc's "super" node with an empty name), whose children are the named children of `<i>`.
struct MallctlNode
{
    const char * name;
    const MallctlNode * children;
    size_t num_children;
    MallctlIndexFunction index;
    MallctlLeafFunction leaf;

    constexpr bool isLeaf() const { return leaf != nullptr; }
    constexpr bool isIndexed() const { return index != nullptr; }
};

/// The root of the tree (jemalloc: `super_root_node`).
extern const MallctlNode mallctl_super_root_node[1];

/// jemalloc: ctl_byname
int mallctlByName(
    ThreadState & thread_state, const char * name, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length);

/// Partial names succeed (the numeric path of the inner node is returned); a too-small `*numeric_path_length_ptr` returns a truncated numeric path.
/// jemalloc: ctl_nametomib
int mallctlNameToNumericPath(ThreadState & thread_state, const char * name, size_t * numeric_path_ptr, size_t * numeric_path_length_ptr);

/// jemalloc: ctl_bymib
int mallctlByNumericPath(
    ThreadState & thread_state,
    const size_t * numeric_path,
    size_t numeric_path_length,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length);

/// Resolves `name` relative to the inner node `numeric_path[0 .. numeric_path_length)`, writing the result to `numeric_path + numeric_path_length`;
/// `*numeric_path_length_ptr` is the capacity of `numeric_path` on input and the total length on output.
/// jemalloc: ctl_mibnametomib
int mallctlExtendNumericPathByName(ThreadState & thread_state, size_t * numeric_path, size_t numeric_path_length, const char * name, size_t * numeric_path_length_ptr);

/// jemalloc: ctl_bymibname
int mallctlByNumericPathAndName(
    ThreadState & thread_state,
    size_t * numeric_path,
    size_t numeric_path_length,
    const char * name,
    size_t * numeric_path_length_ptr,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length);

/// Returns true on error. jemalloc: ctl_boot
bool mallctlBoot();
/// jemalloc: ctl_prefork
void mallctlPrefork(ThreadState * thread_state);
/// jemalloc: ctl_postfork_parent
void mallctlPostforkParent(ThreadState * thread_state);
/// jemalloc: ctl_postfork_child
void mallctlPostforkChild(ThreadState * thread_state);
/// jemalloc: ctl_mtx_assert_held
void mallctlMutexAssertHeld(ThreadState * thread_state);

}
