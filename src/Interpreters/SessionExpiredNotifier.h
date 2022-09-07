#pragma once

#include <vector>
#include <mutex>
#include <functional>
#include <set>
#include <unordered_map>
#include <memory>

#include <base/types.h>

namespace DB
{

class EventNotifier
{
  public:

    struct Handler
    {
      Handler(
        EventNotifier & parent_,
        UInt64 event_id_,
        UInt64 callback_id_)
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
      UInt64 event_id;
      UInt64 callback_id;
    };

    using HandlerPtr = std::shared_ptr<Handler>;

    enum class EventType : UInt64
    {
      ZOOKEEPER_SESSION_EXPIRED = 0,
    };

    template <typename Callback>
    HandlerPtr connect(EventType event, Callback && callback)
    {
      std::lock_guard lock(mutex);
      auto event_id = static_cast<UInt64>(event);
      auto callback_id = calculateIdentifier(event_id, callback_table[event_id].size());

      callback_table[event_id].insert(callback_id);
      storage[callback_id] = std::move(callback);

      return std::make_shared<Handler>(*this, event_id, callback_id);
    }

    void notify(EventType event)
    {
      std::lock_guard lock(mutex);
      for (const auto & identifier : callback_table[static_cast<UInt64>(event)])
        storage[identifier]();
    }

  private:

    using CallbackType = std::function<void()>;
    using CallbackStorage = std::unordered_map<UInt64, CallbackType>;
    using EventToCallbacks = std::vector<std::set<UInt64>>;

    /// Pairing function f: N x N -> N (bijection)
    /// Will return unique numbers given a pair of integers
    UInt64 calculateIdentifier(UInt64 a, UInt64 b)
    {
      return 0.5 * (a + b) * (a + b + 1) + b;
    }

    std::mutex mutex;

    EventToCallbacks callback_table;
    CallbackStorage storage;

};

using EventNotifierPtr = std::shared_ptr<EventNotifier>;

}
