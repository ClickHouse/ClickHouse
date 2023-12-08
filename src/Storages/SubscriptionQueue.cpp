#include <memory>
#include <mutex>
#include <optional>

#include <Processors/Chunk.h>

#include <Storages/SubscriptionQueue.hpp>

namespace DB
{

void Subscriber::push(Chunk chunk) {
  std::unique_lock guard(mutex);
  ready_chunks.emplace_back(std::move(chunk));
}

std::optional<Chunk> Subscriber::extract() {
  std::unique_lock guard(mutex);

  if (ready_chunks.empty()) {
    return std::nullopt;
  }

  Chunk next = std::move(ready_chunks.front());
  ready_chunks.pop_front();

  return next;
}


std::shared_lock<std::shared_mutex> SubscriptionQueue::lockShared() const {
    return std::shared_lock{rwlock};
}

std::unique_lock<std::shared_mutex> SubscriptionQueue::lockExclusive() const {
    return std::unique_lock{rwlock};
}

SubscriberPtr SubscriptionQueue::subscribe() {
  auto sub = std::make_shared<Subscriber>();
  subscribers.push_back(sub);
  return sub;
}

void SubscriptionQueue::pushChunk(Chunk chunk) {
  auto lock = lockShared();

  bool need_clean = false;
  for (const auto& sub : subscribers) {
    auto locked_sub = sub.lock();

    if (locked_sub == nullptr) {
      need_clean = true;
      continue;
    }

    locked_sub->push(chunk.clone());
  }

  if (need_clean) {
    lock.unlock();
    clean();
  }
}

void SubscriptionQueue::clean() {
  auto lock = lockExclusive();
  auto it = subscribers.begin();

  while (it != subscribers.end()) {
    if (it->lock() == nullptr) {
      subscribers.erase(it++);
    } else {
      ++it;
    }
  }
}

}
