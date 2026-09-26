#pragma once

#include <absl/container/flat_hash_set.h>
#include <base/StringViewHash.h>

#include <cstddef>
#include <string_view>

namespace DB
{

using ChildrenSet = absl::flat_hash_set<std::string_view, StringViewHash>;

/// A compact representation of a set of children for Keeper nodes.
/// Uses a tagged-pointer scheme to avoid the 32-byte overhead of
/// absl::flat_hash_set for the common cases of 0 or 1 children.
///
/// Three states discriminated by ptr and name_size:
///   Empty:  ptr == nullptr, name_size == 0
///   Single: ptr != nullptr, name_size > 0  → string_view{ptr, name_size}
///   Set:    ptr != nullptr, name_size == 0  → heap-allocated ChildrenSet*
///
/// Invariant: ZooKeeper child names are always non-empty,
/// so (ptr != nullptr, name_size == 0) unambiguously identifies the set state.
///
/// Callers must guarantee that all string_view data outlives the container.
/// In single mode the pointer is stored directly; in set mode, demotion on
/// erase extracts a string_view before deleting the set. Both cases require
/// stable underlying storage (e.g. arena-owned keys in SnapshotableHashTable).
class CompactChildrenSet
{
public:
    class ConstIterator
    {
    public:
        using value_type = std::string_view;
        using reference = const std::string_view &;
        using pointer = const std::string_view *;
        using difference_type = std::ptrdiff_t;
        using iterator_category = std::forward_iterator_tag;

        ConstIterator() : sv() {}

        reference operator*() const;
        pointer operator->() const;

        ConstIterator & operator++();
        ConstIterator operator++(int);

        bool operator==(const ConstIterator & other) const;
        bool operator!=(const ConstIterator & other) const;

    private:
        friend class CompactChildrenSet;

        /// Constructor for single/empty mode
        ConstIterator(std::string_view sv_, bool done_);

        /// Constructor for set mode
        explicit ConstIterator(ChildrenSet::const_iterator set_it_);

        bool set_mode = false;
        bool done = true;

        union
        {
            /// Used in single mode: holds its own copy of the string_view
            std::string_view sv;

            /// Used in set mode
            ChildrenSet::const_iterator set_it;
        };
    };

    CompactChildrenSet() = default;
    ~CompactChildrenSet();

    CompactChildrenSet(const CompactChildrenSet & other);
    CompactChildrenSet & operator=(const CompactChildrenSet & other);

    CompactChildrenSet(CompactChildrenSet && other) noexcept;
    CompactChildrenSet & operator=(CompactChildrenSet && other) noexcept;

    void insert(std::string_view child);
    void erase(std::string_view child);
    size_t size() const;
    bool empty() const;
    bool contains(std::string_view child) const;
    void clear();
    void reserve(size_t n);

    ConstIterator begin() const;
    ConstIterator end() const;

    /// Returns the number of bytes allocated on the heap for this container.
    /// Zero for empty/single states (inline storage), approximate for set mode.
    size_t heapSizeInBytes() const;

private:
    bool isSet() const { return ptr != nullptr && name_size == 0; }
    bool isSingle() const { return ptr != nullptr && name_size != 0; }
    bool isEmpty() const { return ptr == nullptr; }

    std::string_view asSingle() const { return {ptr, name_size}; }

    const ChildrenSet * asSet() const
    {
        return reinterpret_cast<const ChildrenSet *>(ptr);
    }

    ChildrenSet * asSet()
    {
        return reinterpret_cast<ChildrenSet *>(const_cast<char *>(ptr));
    }

    void promoteToSet(std::string_view existing, std::string_view new_child);

    const char * ptr = nullptr;
    size_t name_size = 0;
};

}
