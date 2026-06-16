#pragma once

#include <vector>
#include <mutex>
#include <functional>
#include <set>
#include <map>
#include <memory>
#include <utility>

#include <base/types.h>
#include <Common/HashTable/Hash.h>


namespace DB
{

class EventNotifier
{
public:
    struct Handler
    {
        Handler(
            EventNotifier & parent_,
            size_t event_id_,
            size_t callback_id_)
            : parent(parent_)
            , event_id(event_id_)
            , callback_id(callback_id_)
        {}

        ~Handler()
        {
          std::lock_guard lock(parent.mutex);

          parent.callback_table[event_id].erase(callback_id);
          parent.storage.erase(callback_id);
        }

    private:
        EventNotifier & parent;
        size_t event_id;
        size_t callback_id;
    };

    using HandlerPtr = std::shared_ptr<Handler>;

    static EventNotifier & init();
    static EventNotifier & instance();
    static void shutdown();

    template <typename EventType, typename Callback>
    [[ nodiscard ]] HandlerPtr subscribe(EventType event, Callback && callback)
    {
        std::lock_guard lock(mutex);

        auto event_id = DefaultHash64(event);
        auto callback_id = calculateIdentifier(event_id, ++counter);

        callback_table[event_id].insert(callback_id);
        storage[callback_id] = std::forward<Callback>(callback);

        return std::make_shared<Handler>(*this, event_id, callback_id);
    }

    template <typename EventType>
    void notify(EventType event)
    {
        std::lock_guard lock(mutex);

        for (const auto & identifier : callback_table[DefaultHash64(event)])
            storage[identifier]();
    }

private:
    // To move boost include for .h file
    static size_t calculateIdentifier(size_t a, size_t b);

    using CallbackType = std::function<void()>;
    using CallbackStorage = std::map<size_t, CallbackType>;
    using EventToCallbacks = std::map<size_t, std::set<size_t>>;

    std::mutex mutex;

    EventToCallbacks callback_table;
    CallbackStorage storage;
    size_t counter{0};

    static std::unique_ptr<EventNotifier> event_notifier;
};

}
