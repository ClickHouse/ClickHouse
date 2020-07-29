#include <iostream>
#include <string>

#include <fmt/format.h>

#include <Core/Types.h>
#include <Common/PODArray.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/Arena.h>
#include <Common/Stopwatch.h>


/** This test program evaluates different solutions for a simple degenerate task:
  * Aggregate data by UInt8 key, calculate "avg" function on Float values.
  *
  * It tests the overhead of various data structures in comparison to the minimal code doing the same task.
  * It also tests what does it cost to access aggregation state via single pointer indirection.
  * Also it evaluates various ways to unroll the loop.
  * And finally it compares with one solution involving bucket sort.
  *
  * How to use:
  *
  * for i in {1..10}; do src/Common/tests/average 100000000 1; done
  *
  * You will find the numbers for various options below.
  */


using namespace DB;

using Float = Float32;

struct State
{
    Float sum = 0;
    size_t count = 0;

    void add(Float value)
    {
        sum += value;
        ++count;
    }

    Float result() const
    {
        return sum / count;
    }

    bool operator!() const
    {
        return !count;
    }
};

using StatePtr = State *;


Float NO_INLINE baseline_baseline(const PODArray<UInt8> & keys, const PODArray<Float> & values)
{
    Arena arena;
    HashMap<UInt8, StatePtr> map;

    size_t size = keys.size();
    for (size_t i = 0; i < size; ++i)
    {
        StatePtr & place = map[keys[i]];
        if (unlikely(!place))
            place = new (arena.alloc<State>()) State();

        place->add(values[i]);
    }

    return map[0]->result();
}


Float NO_INLINE baseline(const PODArray<UInt8> & keys, const PODArray<Float> & values)
{
    Arena arena;
    FixedHashMap<UInt8, StatePtr> map;

    size_t size = keys.size();
    for (size_t i = 0; i < size; ++i)
    {
        StatePtr & place = map[keys[i]];
        if (unlikely(!place))
            place = new (arena.alloc<State>()) State();

        place->add(values[i]);
    }

    return map[0]->result();
}


template <typename Key, typename Mapped>
using FixedImplicitZeroHashMap = FixedHashMap<
    Key,
    Mapped,
    FixedHashMapImplicitZeroCell<Key, Mapped>>;

Float NO_INLINE implicit_zero(const PODArray<UInt8> & keys, const PODArray<Float> & values)
{
    Arena arena;
    FixedImplicitZeroHashMap<UInt8, StatePtr> map;

    size_t size = keys.size();
    for (size_t i = 0; i < size; ++i)
    {
        StatePtr & place = map[keys[i]];
        if (unlikely(!place))
            place = new (arena.alloc<State>()) State();

        place->add(values[i]);
    }

    return map[0]->result();
}


template <typename Key, typename Mapped>
using FixedHashMapWithCalculatedSize = FixedHashMap<
    Key,
    Mapped,
    FixedHashMapCell<Key, Mapped>,
    FixedHashTableCalculatedSize<FixedHashMapCell<Key, Mapped>>>;

Float NO_INLINE calculated_size(const PODArray<UInt8> & keys, const PODArray<Float> & values)
{
    Arena arena;
    FixedHashMapWithCalculatedSize<UInt8, StatePtr> map;

    size_t size = keys.size();
    for (size_t i = 0; i < size; ++i)
    {
        StatePtr & place = map[keys[i]];
        if (unlikely(!place))
            place = new (arena.alloc<State>()) State();

        place->add(values[i]);
    }

    return map[0]->result();
}


Float NO_INLINE implicit_zero_and_calculated_size(const PODArray<UInt8> & keys, const PODArray<Float> & values)
{
    Arena arena;
    FixedImplicitZeroHashMapWithCalculatedSize<UInt8, StatePtr> map;

    size_t size = keys.size();
    for (size_t i = 0; i < size; ++i)
    {
        StatePtr & place = map[keys[i]];
        if (unlikely(!place))
            place = new (arena.alloc<State>()) State();

        place->add(values[i]);
    }

    return map[0]->result();
}

Float NO_INLINE init_out_of_the_loop(const PODArray<UInt8> & keys, const PODArray<Float> & values)
{
    Arena arena;
    FixedImplicitZeroHashMapWithCalculatedSize<UInt8, StatePtr> map;

    for (size_t i = 0; i < 256; ++i)
        map[i] = new (arena.alloc<State>()) State();

    size_t size = keys.size();
    for (size_t i = 0; i < size; ++i)
    {
        StatePtr & place = map[keys[i]];
        place->add(values[i]);
    }

    return map[0]->result();
}

Float NO_INLINE embedded_states(const PODArray<UInt8> & keys, const PODArray<Float> & values)
{
    FixedImplicitZeroHashMapWithCalculatedSize<UInt8, State> map;

    size_t size = keys.size();
    for (size_t i = 0; i < size; ++i)
    {
        State & place = map[keys[i]];
        place.add(values[i]);
    }

    return map[0].result();
}

Float NO_INLINE simple_lookup_table(const PODArray<UInt8> & keys, const PODArray<Float> & values)
{
    Arena arena;
    StatePtr map[256]{};

    size_t size = keys.size();
    for (size_t i = 0; i < size; ++i)
    {
        StatePtr & place = map[keys[i]];
        if (unlikely(!place))
            place = new (arena.alloc<State>()) State();

        place->add(values[i]);
    }

    return map[0]->result();
}

Float NO_INLINE simple_lookup_table_embedded_states(const PODArray<UInt8> & keys, const PODArray<Float> & values)
{
    State map[256]{};

    size_t size = keys.size();
    for (size_t i = 0; i < size; ++i)
        map[keys[i]].add(values[i]);

    return map[0].result();
}

template <size_t UNROLL_COUNT>
Float NO_INLINE unrolled(const PODArray<UInt8> & keys, const PODArray<Float> & values)
{
    Arena arena;
    FixedImplicitZeroHashMapWithCalculatedSize<UInt8, StatePtr> map;

    size_t size = keys.size();
    size_t i = 0;

    size_t size_unrolled = size / UNROLL_COUNT * UNROLL_COUNT;
    for (; i < size_unrolled; i += UNROLL_COUNT)
    {
        StatePtr places[UNROLL_COUNT];
        for (size_t j = 0; j < UNROLL_COUNT; ++j)
        {
            StatePtr & place = map[keys[i + j]];
            if (unlikely(!place))
                place = new (arena.alloc<State>()) State();

            places[j] = place;
        }

        for (size_t j = 0; j < UNROLL_COUNT; ++j)
            places[j]->add(values[i + j]);
    }

    for (; i < size; ++i)
    {
        StatePtr & place = map[keys[i]];
        if (unlikely(!place))
            place = new (arena.alloc<State>()) State();

        place->add(values[i]);
    }

    return map[0]->result();
}

template <size_t UNROLL_COUNT>
Float NO_INLINE simple_lookup_table_unrolled(const PODArray<UInt8> & keys, const PODArray<Float> & values)
{
    Arena arena;
    StatePtr map[256]{};

    size_t size = keys.size();
    size_t i = 0;

    size_t size_unrolled = size / UNROLL_COUNT * UNROLL_COUNT;
    for (; i < size_unrolled; i += UNROLL_COUNT)
    {
        StatePtr places[UNROLL_COUNT];
        for (size_t j = 0; j < UNROLL_COUNT; ++j)
        {
            StatePtr & place = map[keys[i + j]];
            if (unlikely(!place))
                place = new (arena.alloc<State>()) State();

            places[j] = place;
        }

        for (size_t j = 0; j < UNROLL_COUNT; ++j)
            places[j]->add(values[i + j]);
    }

    for (; i < size; ++i)
    {
        StatePtr & place = map[keys[i]];
        if (unlikely(!place))
            place = new (arena.alloc<State>()) State();

        place->add(values[i]);
    }

    return map[0]->result();
}

template <size_t UNROLL_COUNT>
Float NO_INLINE embedded_states_unrolled(const PODArray<UInt8> & keys, const PODArray<Float> & values)
{
    FixedImplicitZeroHashMapWithCalculatedSize<UInt8, State> map;

    size_t size = keys.size();
    size_t i = 0;

    size_t size_unrolled = size / UNROLL_COUNT * UNROLL_COUNT;
    for (; i < size_unrolled; i += UNROLL_COUNT)
    {
        StatePtr places[UNROLL_COUNT];
        for (size_t j = 0; j < UNROLL_COUNT; ++j)
            places[j] = &map[keys[i + j]];

        for (size_t j = 0; j < UNROLL_COUNT; ++j)
            places[j]->add(values[i + j]);
    }

    for (; i < size; ++i)
    {
        State & place = map[keys[i]];
        place.add(values[i]);
    }

    return map[0].result();
}

template <size_t UNROLL_COUNT>
Float NO_INLINE simple_lookup_table_embedded_states_unrolled(const PODArray<UInt8> & keys, const PODArray<Float> & values)
{
    State map[256]{};

    size_t size = keys.size();
    size_t i = 0;

    size_t size_unrolled = size / UNROLL_COUNT * UNROLL_COUNT;
    for (; i < size_unrolled; i += UNROLL_COUNT)
    {
        StatePtr places[UNROLL_COUNT];
        for (size_t j = 0; j < UNROLL_COUNT; ++j)
            places[j] = &map[keys[i + j]];

        for (size_t j = 0; j < UNROLL_COUNT; ++j)
            places[j]->add(values[i + j]);
    }

    for (; i < size; ++i)
    {
        State & place = map[keys[i]];
        place.add(values[i]);
    }

    return map[0].result();
}


template <size_t UNROLL_COUNT>
Float NO_INLINE microsort(const PODArray<UInt8> & keys, const PODArray<Float> & values)
{
    State map[256]{};

    size_t size = keys.size();

    /// Calculate histograms of keys.

    using CountType = UInt32;

    static constexpr size_t HISTOGRAM_SIZE = 256;

    CountType count[HISTOGRAM_SIZE * UNROLL_COUNT]{};
    size_t unrolled_size = size / UNROLL_COUNT * UNROLL_COUNT;

    for (const UInt8 * elem = keys.data(); elem < keys.data() + unrolled_size; elem += UNROLL_COUNT)
        for (size_t i = 0; i < UNROLL_COUNT; ++i)
            ++count[i * HISTOGRAM_SIZE + elem[i]];

    for (const UInt8 * elem = keys.data() + unrolled_size; elem < keys.data() + size; ++elem)
        ++count[*elem];

    for (size_t i = 0; i < HISTOGRAM_SIZE; ++i)
        for (size_t j = 1; j < UNROLL_COUNT; ++j)
            count[i] += count[j * HISTOGRAM_SIZE + i];

    /// Row indices in a batch for each key.

    PODArray<UInt32> indices(size);
    UInt32 * positions[HISTOGRAM_SIZE];
    positions[0] = indices.data();

    for (size_t i = 1; i < HISTOGRAM_SIZE; ++i)
        positions[i] = positions[i - 1] + count[i - 1];

    for (size_t i = 0; i < size; ++i)
        *positions[keys[i]]++ = i;

    /// Update states.

    UInt32 * idx = indices.data();
    for (size_t i = 0; i < HISTOGRAM_SIZE; ++i)
        for (; idx < positions[i]; ++idx)
            map[i].add(values[*idx]);

    return map[0].result();
}


int main(int argc, char ** argv)
{
    size_t size = argc > 1 ? std::stoull(argv[1]) : 1000000000;
    size_t variant = argc > 2 ? std::stoull(argv[2]) : 1;

    PODArray<UInt8> keys(size);
    PODArray<Float> values(size);

    /// Fill source data
    for (size_t i = 0; i < size; ++i)
    {
        keys[i] = __builtin_ctz(i + 1); /// Make keys to have just slightly more realistic distribution.
        values[i] = i; /// The distribution of values does not affect execution speed.
    }

    /// Aggregate
    Stopwatch watch;
    Float res;

    switch (variant)
    {
        case 0: res = baseline(keys, values); break;
        case 1: res = implicit_zero(keys, values); break;
        case 2: res = calculated_size(keys, values); break;
        case 3: res = implicit_zero_and_calculated_size(keys, values); break;
        case 4: res = init_out_of_the_loop(keys, values); break;
        case 5: res = embedded_states(keys, values); break;
        case 6: res = simple_lookup_table(keys, values); break;
        case 7: res = simple_lookup_table_embedded_states(keys, values); break;
        case 8: res = microsort<1>(keys, values); break;
        case 9: res = baseline_baseline(keys, values); break;

        case 32: res = unrolled<2>(keys, values); break;
        case 34: res = unrolled<4>(keys, values); break;
        case 36: res = unrolled<6>(keys, values); break;
        case 38: res = unrolled<8>(keys, values); break;
        case 316: res = unrolled<16>(keys, values); break;

        case 52: res = embedded_states_unrolled<2>(keys, values); break;
        case 54: res = embedded_states_unrolled<4>(keys, values); break;
        case 56: res = embedded_states_unrolled<6>(keys, values); break;
        case 58: res = embedded_states_unrolled<8>(keys, values); break;
        case 516: res = embedded_states_unrolled<16>(keys, values); break;

        case 62: res = simple_lookup_table_unrolled<2>(keys, values); break;
        case 64: res = simple_lookup_table_unrolled<4>(keys, values); break;
        case 66: res = simple_lookup_table_unrolled<6>(keys, values); break;
        case 68: res = simple_lookup_table_unrolled<8>(keys, values); break;
        case 616: res = simple_lookup_table_unrolled<16>(keys, values); break;

        case 72: res = simple_lookup_table_embedded_states_unrolled<2>(keys, values); break;
        case 74: res = simple_lookup_table_embedded_states_unrolled<4>(keys, values); break;
        case 76: res = simple_lookup_table_embedded_states_unrolled<6>(keys, values); break;
        case 78: res = simple_lookup_table_embedded_states_unrolled<8>(keys, values); break;
        case 716: res = simple_lookup_table_embedded_states_unrolled<16>(keys, values); break;

        case 82: res = microsort<2>(keys, values); break;
        case 84: res = microsort<4>(keys, values); break;
        case 86: res = microsort<6>(keys, values); break;
        case 88: res = microsort<8>(keys, values); break;
        case 816: res = microsort<16>(keys, values); break;

        default: break;
    }

    watch.stop();
    fmt::print("Aggregated (res = {}) in {} sec., {} million rows/sec., {} MiB/sec.\n",
        res,
        watch.elapsedSeconds(),
        size_t(size / watch.elapsedSeconds() / 1000000),
        size_t(size * (sizeof(Float) + sizeof(UInt8)) / watch.elapsedSeconds() / 1000000));

    return 0;
}

