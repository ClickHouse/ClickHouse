#pragma once

#include <cstddef>
#include <cassert>
#include <random>
#include <vector>
#include <pcg_random.hpp>

#if defined(TEST_WEIGHTED_RANDOM_QUEUE)
#include <iostream>
#endif


namespace DB
{

/** Allow to pseudorandomly select from a set of items
  * with probability proportional to their weights,
  * while dynamically adding new items.
  */
template <typename Value, typename Weight = uint64_t, typename RandomGenerator = pcg64_fast>
class WeightedRandomQueue
{
public:
    template <typename... Params>
    WeightedRandomQueue(Params &&... params) : rng(std::forward<Params>(params)...)
    {
    }

    void push(Value && value, Weight weight)
    {
        assert(weight > 0);

        size_t idx = nodes.size();
        nodes.emplace_back(std::move(value), weight);

        updateParents(idx, weight);
    }

    /// Extract pseudorandom value from the queue.
    Value pop()
    {
        return popAt(sampleIndex());
    }

    /// Observe pseudorandom value.
    Value & sample()
    {
        return nodes[sampleIndex()];
    }

    const Value & sample() const
    {
        return nodes[sampleIndex()];
    }

    size_t size() const
    {
        return nodes.size();
    }

    bool empty() const
    {
        return nodes.empty();
    }

    /// Select pseudorandom value. Pass it to function f.
    /// The function returns a pair with result and a flag, should we remove the value from the queue.
    template <typename F>
    auto extract(F && f)
    {
        size_t idx = sampleIndex();
        auto [res, need_remove] = f(nodes[idx]);
        if (need_remove)
            popAt(idx);
        return res;
    }

private:
    struct Node
    {
        Value value;
        /// Weight of itself + weights of left and right children.
        Weight weights = 0;

        Node() = default;

        Node(Value && value_, Weight weight_)
            : value(std::move(value_)), weights(weight_)
        {
        }
    };

    /// Array of nodes forms implicit binary tree (binary heap).
    std::vector<Node> nodes;
    RandomGenerator rng;

    static size_t left(size_t i) { return 2 * i + 1; }
    static size_t right(size_t i) { return 2 * i + 2; }
    static size_t parent(size_t i) { return (i - 1) / 2; }

    /// Adds weight to all parent nodes.
    void updateParents(size_t i, Weight add_weight)
    {
        while (i)
        {
            i = parent(i);
            nodes[i].weights += add_weight;
        }
    }

    /// Removes last node and returns it.
    Node removeLast()
    {
        size_t idx = nodes.size() - 1;
        Node last = std::move(nodes[idx]);
        nodes.pop_back();
        updateParents(idx, -last.weights);
        return last;
    }

    /// Remove node at idx and return the value.
    Value popAt(size_t idx)
    {
        Node res = std::move(nodes[idx]);

        /// In fact, the removed node is replaced by the last node.
        nodes[idx] = removeLast();

        size_t nodes_size = nodes.size();
        size_t left_idx = left(idx);
        size_t right_idx = right(idx);

        size_t left_weights = left_idx < nodes_size ? nodes[left_idx].weights : 0;
        size_t right_weights = right_idx < nodes_size ? nodes[right_idx].weights : 0;

        size_t children_weights = left_weights + right_weights;
        size_t res_weight = res.weights - children_weights;

        Weight weight_diff = nodes[idx].weights - res_weight;

        nodes[idx].weights += children_weights;
        updateParents(idx, weight_diff);

        return res.value;
    }

    size_t find(Weight cutoff, size_t idx) const
    {
        size_t nodes_size = nodes.size();

        size_t left_idx = left(idx);
        if (left_idx < nodes_size && cutoff < nodes[left_idx].weights)
            return find(cutoff, left_idx);

        size_t right_idx = right(idx);
        if (right_idx < nodes_size)
        {
            Weight sum_weights_left_and_self = nodes[idx].weights - nodes[right_idx].weights;
            if (cutoff >= sum_weights_left_and_self)
                return find(cutoff - sum_weights_left_and_self, right_idx);
        }

        return idx;
    }

    size_t sampleIndex()
    {
        assert(!empty());

        Weight cutoff = std::uniform_int_distribution<Weight>(0, nodes[0].weights - 1)(rng);
        return find(cutoff, 0);
    }

#if defined(TEST_WEIGHTED_RANDOM_QUEUE)
public:
    void print() const
    {
        for (const auto & node : nodes)
            std::cerr << node.value << ", " << node.weights << "\n";
    }
#endif
};

}
