#include <Common/FiberLocal.h>

thread_local constinit FiberLocalStorage FiberLocalStorage::thread_storage{};
constinit std::array<std::atomic<void (*)(void *)>, FiberLocalStorage::slot_count> FiberLocalStorage::slot_destructors{};

void FiberLocalStorage::destroySlots() noexcept
{
    for (size_t i = 0; i < slot_count; ++i)
    {
        void (* destroy)(void *) = slot_destructors[i].load(std::memory_order_relaxed);
        if (!destroy)
            continue;
        if (void * object = reinterpret_cast<void *>(slots[i]))
        {
            slots[i] = 0;
            destroy(object);
        }
    }
}

void FiberLocalStorage::SlotsDestroyer::operator()(FiberLocalStorage * storage) const noexcept
{
    storage->destroySlots();
    delete storage;
}

FiberLocalStorage::Holder FiberLocalStorage::create()
{
    return Holder(new FiberLocalStorage());
}

void FiberLocalStorage::registerDestructor(size_t slot, void (* destroy)(void *)) noexcept
{
    slot_destructors[slot].store(destroy, std::memory_order_relaxed);
}

struct FiberLocalStorage::ThreadStorageCleaner
{
    ~ThreadStorageCleaner()
    {
        thread_storage.destroySlots();
    }
};

void FiberLocalStorage::armThreadStorageCleaner() noexcept
{
    [[maybe_unused]] static thread_local ThreadStorageCleaner cleaner;
}
