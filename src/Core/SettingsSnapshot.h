#pragma once

#include <Core/SettingsMetrics.h>
#include <Common/MemoryTrackerBlockerInThread.h>

#include <array>
#include <cstddef>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace DB
{

bool settingsAllocationIsServerOwned();

/// Remember server-owned allocation scopes through the last shared-owner destruction.
/// Query-local allocations retain the ordinary tracking used by other shared query values.
/// Exclude embedded objects that account for their own bytes, also after allocator rebinding.
template <typename T, SettingsAllocationKind kind = SettingsAllocationKind::SnapshotState, size_t separately_accounted_bytes = 0>
struct SettingsSnapshotAllocator
{
    using value_type = T;
    using is_always_equal = std::false_type;

    template <typename U>
    struct rebind
    {
        using other = SettingsSnapshotAllocator<U, kind, separately_accounted_bytes>;
    };

    bool server_owned;

    explicit SettingsSnapshotAllocator(bool server_owned_ = settingsAllocationIsServerOwned())
        : server_owned(server_owned_)
    {
    }

    template <typename U>
    explicit SettingsSnapshotAllocator(const SettingsSnapshotAllocator<U, kind, separately_accounted_bytes> & other)
        : server_owned(other.server_owned)
    {
    }

    T * allocate(size_t count)
    {
        static_assert(separately_accounted_bytes <= sizeof(T));
        auto guard = blockServerMemory();
        auto * result = std::allocator<T>{}.allocate(count);
        accountSettingsAllocation(kind, count * (sizeof(T) - separately_accounted_bytes), 1);
        return result;
    }

    void deallocate(T * pointer, size_t count)
    {
        auto guard = blockServerMemory();
        std::allocator<T>{}.deallocate(pointer, count);
        accountSettingsAllocation(kind, -static_cast<Int64>(count * (sizeof(T) - separately_accounted_bytes)), -1);
    }

    template <typename U, typename... Args>
    void construct(U * pointer, Args &&... args)
    {
        auto guard = blockServerMemory();
        std::construct_at(pointer, std::forward<Args>(args)...);
    }

    template <typename U>
    void destroy(U * pointer)
    {
        auto guard = blockServerMemory();
        std::destroy_at(pointer);
    }

    template <typename U>
    bool operator==(const SettingsSnapshotAllocator<U, kind, separately_accounted_bytes> & other) const
    {
        return server_owned == other.server_owned;
    }

private:
    std::optional<MemoryTrackerBlockerInThread> blockServerMemory() const
    {
        if (server_owned)
            return std::make_optional<MemoryTrackerBlockerInThread>();
        return std::nullopt;
    }
};

struct SettingsSnapshotField
{
    size_t offset;
    size_t size;
    void (*construct)(void *, const void *);
    void (*destroy)(void *);
};

template <typename T>
constexpr SettingsSnapshotField settingsSnapshotField(size_t offset)
{
    static_assert(alignof(T) <= alignof(std::max_align_t));
    return {
        offset,
        sizeof(T),
        [](void * destination, const void * source) { std::construct_at(static_cast<T *>(destination), *static_cast<const T *>(source)); },
        [](void * pointer) { std::destroy_at(static_cast<T *>(pointer)); },
    };
}

/// A fixed-depth snapshot: a shared read table owns independently shared typed chunks.
/// Copying never builds a parent chain. A write clones the table and only its affected chunk.
template <typename Description, size_t chunk_size = 512>
class SettingsSnapshot
{
    using Data = typename Description::Data;
    static_assert(chunk_size && !(chunk_size & (chunk_size - 1)) && chunk_size >= alignof(Data));
    static constexpr size_t num_chunks = (sizeof(Data) + chunk_size - 1) / chunk_size;
    static constexpr size_t num_fields = Description::fields.size();

    struct Layout
    {
        std::array<size_t, num_chunks + 1> boundaries{};
        std::array<size_t, num_chunks> bytes{};
        std::array<size_t, num_fields> members{};
    };

    static constexpr Layout makeLayout()
    {
        Layout result;
        for (const auto & field : Description::fields)
        {
            const size_t index = field.offset / chunk_size;
            ++result.boundaries[index + 1];
            const size_t end = field.offset % chunk_size + field.size;
            if (end > result.bytes[index])
                result.bytes[index] = end;
        }
        for (size_t index = 1; index <= num_chunks; ++index)
            result.boundaries[index] += result.boundaries[index - 1];
        auto positions = result.boundaries;
        for (size_t field = 0; field < num_fields; ++field)
            result.members[positions[Description::fields[field].offset / chunk_size]++] = field;
        return result;
    }

    static constexpr Layout layout = makeLayout();

    struct Chunk
    {
        const size_t index;
        std::unique_ptr<std::byte[]> buffer;
        size_t constructed = 0;

        template <typename Getter>
        Chunk(size_t index_, Getter get)
            : index(index_)
            , buffer(std::make_unique_for_overwrite<std::byte[]>(layout.bytes[index]))
        {
            accountSettingsStructuralBytes(layout.bytes[index]);
            try
            {
                for (size_t position = layout.boundaries[index]; position < layout.boundaries[index + 1]; ++position)
                {
                    const auto & field = Description::fields[layout.members[position]];
                    field.construct(buffer.get() + field.offset % chunk_size, get(field.offset));
                    ++constructed;
                }
            }
            catch (...)
            {
                destroyFields();
                accountSettingsStructuralBytes(-static_cast<Int64>(layout.bytes[index]));
                throw;
            }
        }

        void destroyFields()
        {
            while (constructed)
            {
                --constructed;
                const auto & field = Description::fields[layout.members[layout.boundaries[index] + constructed]];
                field.destroy(buffer.get() + field.offset % chunk_size);
            }
        }

        ~Chunk()
        {
            destroyFields();
            accountSettingsStructuralBytes(-static_cast<Int64>(layout.bytes[index]));
        }
    };

    struct State
    {
        std::shared_ptr<const Data> defaults;
        std::array<std::shared_ptr<const std::byte>, num_chunks> values{};
        std::array<bool, num_chunks> server_chunks{};
        const bool server_owned;

        explicit State(std::shared_ptr<const Data> defaults_)
            : defaults(std::move(defaults_))
            , server_owned(true)
        {
            /// The state owns `defaults`; empty control blocks identify immutable borrowed chunks.
            server_chunks.fill(true);
            for (size_t index = 0; index < num_chunks; ++index)
                values[index] = std::shared_ptr<const std::byte>(
                    std::shared_ptr<const Data>{}, reinterpret_cast<const std::byte *>(defaults.get()) + index * chunk_size);
        }

        State(const State & other, bool server_owned_)
            : defaults(other.defaults)
            , values(other.values)
            , server_chunks(other.server_chunks)
            , server_owned(server_owned_)
        {
        }
    };

    static const std::shared_ptr<State> & defaultState()
    {
        /// Own a typed copy so the snapshot's values can outlive the accessor singleton.
        static const auto result = std::allocate_shared<State>(
            SettingsSnapshotAllocator<State>{true},
            std::allocate_shared<const Data>(SettingsSnapshotAllocator<Data, SettingsAllocationKind::DenseData>{true}));
        return result;
    }

    std::shared_ptr<State> state = defaultState();

public:
    bool sharesStorageWith(const SettingsSnapshot & other) const { return state == other.state; }

    bool hasServerOwnedStorage() const
    {
        if (!state->server_owned)
            return false;
        for (bool server_owned : state->server_chunks)
            if (!server_owned)
                return false;
        return true;
    }

    const void * getSettingPointer(size_t offset) const { return state->values[offset / chunk_size].get() + offset % chunk_size; }

    void * getSettingPointer(size_t offset)
    {
        const bool server_owned = settingsAllocationIsServerOwned();
        if (state.use_count() != 1 || state->server_owned != server_owned)
            state = std::allocate_shared<State>(SettingsSnapshotAllocator<State>{server_owned}, *state, server_owned);

        const size_t index = offset / chunk_size;
        auto & owner = state->values[index];
        /// Zero owners means a borrowed default; one means a uniquely owned mutable chunk.
        if (owner.use_count() != 1 || state->server_chunks[index] != server_owned)
        {
            auto replacement = std::allocate_shared<Chunk>(
                SettingsSnapshotAllocator<Chunk, SettingsAllocationKind::SnapshotChunk>{server_owned},
                index,
                [this](size_t position) { return std::as_const(*this).getSettingPointer(position); });
            const auto * address = replacement->buffer.get();
            owner = std::shared_ptr<const std::byte>(std::move(replacement), address);
            state->server_chunks[index] = server_owned;
        }
        return const_cast<std::byte *>(owner.get()) + offset % chunk_size;
    }
};

}
