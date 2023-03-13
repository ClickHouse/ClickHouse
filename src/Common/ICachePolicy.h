#pragma once

#include <functional>
#include <memory>
#include <mutex>

namespace DB
{

template <typename T>
struct EqualWeightFunction
{
    size_t operator()(const T &) const
    {
        return 1;
    }
};

template <typename Key, typename Mapped, typename HashFunction = std::hash<Key>, typename WeightFunction = EqualWeightFunction<Mapped>>
class ICachePolicy
{
public:
    using MappedPtr = std::shared_ptr<Mapped>;
    using OnWeightLossFunction = std::function<void(size_t)>;

    virtual size_t weight(std::lock_guard<std::mutex> & /* cache_lock */) const = 0;
    virtual size_t count(std::lock_guard<std::mutex> & /* cache_lock */) const = 0;
    virtual size_t maxSize() const = 0;

    virtual void reset(std::lock_guard<std::mutex> & /* cache_lock */) = 0;
    virtual void remove(const Key & key, std::lock_guard<std::mutex> & /* cache_lock */) = 0;
    virtual MappedPtr get(const Key & key, std::lock_guard<std::mutex> & /* cache_lock */) = 0;
    virtual void set(const Key & key, const MappedPtr & mapped, std::lock_guard<std::mutex> & /* cache_lock */) = 0;

    virtual ~ICachePolicy() = default;
};

}
