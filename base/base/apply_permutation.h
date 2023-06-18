#pragma once

#include <ranges>


/// Transforms the two item sequences according to index sequence order.
/// Order sequence needs to be exactly a permutation of the sequence [0, 1, ... , N],
/// where N is the biggest index in the item sequence (zero-indexed).
/// This algorithm is like boost::algorithm::apply_permutation() but it works for two sequences of items.
template <
    std::ranges::random_access_range Items1,
    std::ranges::random_access_range Items2,
    std::ranges::random_access_range Indices>
void apply_permutation(Items1 & items1, Items2 & items2, Indices && indices)
{
    size_t size = std::size(indices);
    for (size_t i = 0; i != size; i++)
    {
        size_t current = i;
        while (i != indices[current])
        {
            size_t next = indices[current];
            std::swap(items1[current], items1[next]);
            std::swap(items2[current], items2[next]);
            indices[current] = current;
            current = next;
        }
        indices[current] = current;
    }
}

/// Transforms the two three sequences according to index sequence order.
/// Order sequence needs to be exactly a permutation of the sequence [0, 1, ... , N],
/// where N is the biggest index in the item sequence (zero-indexed).
/// This algorithm is like boost::algorithm::apply_permutation() but it works for three sequences of items.
template <
    std::ranges::random_access_range Items1,
    std::ranges::random_access_range Items2,
    std::ranges::random_access_range Items3,
    std::ranges::random_access_range Indices>
void apply_permutation(Items1 & items1, Items2 & items2, Items3 & items3, Indices && indices)
{
    size_t size = std::size(indices);
    for (size_t i = 0; i != size; i++)
    {
        size_t current = i;
        while (i != indices[current])
        {
            size_t next = indices[current];
            std::swap(items1[current], items1[next]);
            std::swap(items2[current], items2[next]);
            std::swap(items3[current], items3[next]);
            indices[current] = current;
            current = next;
        }
        indices[current] = current;
    }
}
