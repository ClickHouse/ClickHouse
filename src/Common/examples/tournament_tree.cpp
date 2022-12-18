#include <iomanip>
#include <base/bit_cast.h>
#include <pcg_random.hpp>

#include <Core/Defines.h>
#include <Common/BitHelpers.h>
#include <Common/Stopwatch.h>
#include <Common/TournamentTree.h>
#include <Common/randomSeed.h>
#include <Common/thread_local_rng.h>


using namespace DB;

static size_t compare_count = 0;

class IntegerArrayCursor
{
public:
    bool isValid() const { return values_index < values.size(); }

    bool isLast() const { return values_index == values.size() - 1; }

    void next()
    {
        assert(isValid());
        ++values_index;
    }

    Int64 value() { return values[values_index]; }

    bool operator<(const IntegerArrayCursor & rhs) const
    {
        assert(isValid());
        assert(rhs.isValid());

        ++compare_count;

        return values[values_index] > rhs.values[rhs.values_index];
    }

    bool empty() const { return values.empty(); }

    void print() { std::cout << "Cursor value " << values[values_index] << std::endl; }

    void dump()
    {
        for (auto & value : values)
            std::cout << value << ' ';

        std::cout << std::endl;
    }

    explicit IntegerArrayCursor(std::vector<Int64> values_) : values(std::move(values_)) { }

private:
    std::vector<Int64> values;
    size_t values_index = 0;
};

template <typename Cursor>
class SortingHeapUpdated
{
public:
    SortingHeapUpdated() = default;

    template <typename Cursors>
    explicit SortingHeapUpdated(Cursors & cursors) : queue(cursors)
    {
        std::make_heap(queue.begin(), queue.end());
    }

    bool isValid() const { return !queue.empty(); }

    Cursor & current() { return queue.front(); }

    size_t size() { return queue.size(); }

    Cursor & nextChild() { return queue[nextChildIndex()]; }

    void next()
    {
        assert(isValid());

        if (!current().isLast())
        {
            current().next();
            updateTop();
        }
        else
        {
            removeTop();
        }
    }

    void replaceTop(Cursor new_top)
    {
        current() = new_top;
        updateTop();
    }

    void removeTop()
    {
        std::pop_heap(queue.begin(), queue.end());
        queue.pop_back();
        next_idx = 0;
    }

private:
    using Container = std::vector<Cursor>;
    Container queue;

    /// Cache comparison between first and second child if the order in queue has not been changed.
    size_t next_idx = 0;

    size_t nextChildIndex()
    {
        if (next_idx == 0)
        {
            next_idx = 1;

            if (queue.size() > 2 && queue[1] < queue[2])
                ++next_idx;
        }

        return next_idx;
    }

    /// This is adapted version of the function __sift_down from libc++.
    /// Why cannot simply use std::priority_queue?
    /// - because it doesn't support updating the top element and requires pop and push instead.
    /// Also look at "Boost.Heap" library.
    void updateTop()
    {
        size_t size = queue.size();
        if (size < 2)
            return;

        auto begin = queue.begin();

        size_t child_idx = nextChildIndex();
        auto child_it = begin + child_idx;

        /// Check if we are in order.
        if (*child_it < *begin)
            return;

        next_idx = 0;

        auto curr_it = begin;
        auto top(std::move(*begin));
        do
        {
            /// We are not in heap-order, swap the parent with it's largest child.
            *curr_it = std::move(*child_it);
            curr_it = child_it;

            // recompute the child based off of the updated parent
            child_idx = 2 * child_idx + 1;

            if (child_idx >= size)
                break;

            child_it = begin + child_idx;

            if ((child_idx + 1) < size && *child_it < *(child_it + 1))
            {
                /// Right child exists and is greater than left child.
                ++child_it;
                ++child_idx;
            }

            /// Check if we are in order.
        } while (!(*child_it < top));
        *curr_it = std::move(top);
    }
};

IntegerArrayCursor generateRandomCursor(size_t cursor_length)
{
    std::uniform_int_distribution<Int64> distribution(0, std::numeric_limits<Int64>::max());
    std::vector<Int64> result;

    for (size_t i = 0; i < cursor_length; ++i)
    {
        Int64 value = distribution(thread_local_rng);
        result.emplace_back(value);
    }

    std::sort(result.begin(), result.end());

    return IntegerArrayCursor(std::move(result));
}

template <typename StrategyTreeType>
Int64 TestStrategy(const std::vector<IntegerArrayCursor> & cursors)
{
    StrategyTreeType tree(cursors);

    Int64 sum = 0;

    while (tree.isValid())
    {
        auto & current = tree.current();
        sum += current.value();
        tree.next();
    }

    return sum;
}

/** Benchmark to check binary heap and tournament tree performance for task of k way merge sort.
  * https://en.wikipedia.org/wiki/K-way_merge_algorithm
  *
  * In results for such simple compare function that potentially could be inlined.
  * Both heap and tournament tree perform almost the same related to CPU time.
  *
  * Related to compare functions calls heap always has 1.5 more compares than tournament tree.
  */
int main(int argc, char ** argv)
{
    if (argc < 2)
    {
        std::cerr << "./k_way_merge strategy [iterations]" << std::endl;
        return 1;
    }

    size_t strategy = std::stoull(argv[1]);
    size_t iterations = argc == 3 ? std::stoull(argv[2]) : 1;

    std::vector<IntegerArrayCursor> cursors;

    static constexpr size_t cursors_size = 64;
    for (size_t i = 0; i < cursors_size; ++i)
    {
        cursors.emplace_back(generateRandomCursor(10000));
    }

    UInt64 sum_total = 0;
    Stopwatch watch;

    if (strategy == 1)
    {
        std::cout << "Using heap" << std::endl;

        watch.start();

        for (size_t i = 0; i < iterations; ++i)
        {
            UInt64 sum = TestStrategy<SortingHeapUpdated<IntegerArrayCursor>>(cursors);
            sum_total += sum;
        }

        watch.stop();
    }
    else if (strategy == 2)
    {
        std::cout << "Using tournament tree" << std::endl;

        watch.start();

        for (size_t i = 0; i < iterations; ++i)
        {
            UInt64 sum = TestStrategy<TournamentTree<IntegerArrayCursor>>(cursors);
            sum_total += sum;
        }

        watch.stop();
    }
    else
    {
        std::cerr << "Wrong strategy expected 1 or 2" << std::endl;
        return 1;
    }

    std::cout << "Compare count " << compare_count << std::endl;
    compare_count = 0;

    std::cout << "Sum " << sum_total << std::endl;
    std::cout << "Elapsed " << watch.elapsedMicroseconds() << std::endl;

    return 0;
}
