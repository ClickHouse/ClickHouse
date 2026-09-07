#pragma once

#include <atomic>

namespace DB
{

struct UpdateInboxEntry
{
    UpdateInboxEntry() = default;
    UpdateInboxEntry(const UpdateInboxEntry &) {}
    UpdateInboxEntry & operator=(const UpdateInboxEntry &) = delete;

    template <class T>
    T * as() { return static_cast<T *>(this); }

    UpdateInboxEntry * next = nullptr;
    std::atomic<bool> already_in_inbox{false};
};

}
