#pragma once

#include <Common/Cache/Cache.h>

namespace DB
{
template <
    typename TKey,
    typename TMapped,
    typename HashFunction = std::hash<TKey>,
    typename WeightFunction = ITrivialWeightFunction<TMapped>>
class LFUCache : virtual public Cache<TKey, TMapped, HashFunction, WeightFunction>
{
private:
    using Base = Cache<TKey, TMapped, HashFunction, WeightFunction>;

public:
    using Key = typename Base::Key;
    using Mapped = typename Base::Mapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    LFUCache(size_t max_size_ = 1) : Base(max_size_) { }


protected:
    using KeyQueue = std::list<Key>;
    using KeyQueueIterator = typename KeyQueue::iterator;

    using Freqs = std::unordered_map<Key, size_t, HashFunction>;
    using FreqsInverted = std::map<size_t, KeyQueue>;
    using FreqsInvertedIterator = typename FreqsInverted::iterator;

    Freqs freqs;
    FreqsInverted freqs_inverted;


    struct Cell : virtual public Base::Cell
    {
        FreqsInvertedIterator inverted_freq_iterator;
        KeyQueueIterator sublist_iterator;
    };

    void resetImpl() override
    {
        Base::resetImpl();
        freqs.clear();
        freqs_inverted.clear();
    }

    virtual void removeInvalid() override { }

    virtual void removeOverflow() override
    {
        while ((Base::current_size > Base::max_size) && (freqs.size()))
        {
            auto it = freqs_inverted.begin();
            const Key & key = it->first;
            this->deleteImpl(key);
        }

        if (Base::current_size > (1ull << 63))
        {
            LOG_ERROR(&Poco::Logger::get("LFUCache"), "LFUCache became inconsistent. There must be a bug in it.");
            abort();
        }
    }

    virtual void updateStructuresGetOrSet(
        [[maybe_unused]] const Key & key,
        [[maybe_unused]] std::shared_ptr<typename Base::Cell> & cell_ptr,
        [[maybe_unused]] bool new_element = false,
        [[maybe_unused]] UInt64 ttl = 2) override
    {
        auto cell = std::dynamic_pointer_cast<Cell>(cell_ptr);

        size_t frequency = 0;
        {
            auto [freq_it, inserted] = freqs.insert({key, frequency});
            frequency = ++freq_it->second;
        }
        auto [counter_bin_new, inserted] = freqs_inverted.insert({frequency, {}});

        if (new_element)
            cell->sublist_iterator = counter_bin_new->second.insert(counter_bin_new->second.end(), key);
        else
        {
            auto counter_bin_old = cell->inverted_freq_iterator;
            counter_bin_new->second.splice(counter_bin_new->second.end(), counter_bin_old->second, cell->sublist_iterator);
        }

        cell->inverted_freq_iterator = counter_bin_new;
    }

    virtual void updateStructuresDelete([[maybe_unused]] const Key & key, [[maybe_unused]] std::shared_ptr<typename Base::Cell> & cell_ptr) override
    {
        auto cell = std::dynamic_pointer_cast<Cell>(cell_ptr);

        freqs.erase(key);
        cell->inverted_freq_iterator->second.erase(cell->sublist_iterator);

        if (!cell->inverted_freq_iterator->second.size())
            freqs_inverted.erase(cell->inverted_freq_iterator);
    }
};

}
