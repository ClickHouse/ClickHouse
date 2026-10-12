#pragma once

namespace DB
{

namespace Runtime::V2
{

struct UpdateInboxEntry
{
    UpdateInboxEntry() = default;
    UpdateInboxEntry(const UpdateInboxEntry &) {}
    UpdateInboxEntry & operator=(const UpdateInboxEntry &) = delete;

    template <class T>
    T * as() { return static_cast<T *>(this); }

    UpdateInboxEntry * next = nullptr;
};

}

}
