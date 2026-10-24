/// Compares `PairingHeap` with jemalloc's `ph.h` (instantiated in pairing_heap_oracle_ref.c) on long randomized
/// traces: the result of every operation and the complete tree shape (every link of every node) must be identical.

#include <allocator/PairingHeap.h>

#include "Test.h"

using namespace jemalloc;

extern "C" {
void ref_pairing_heap_init(int n, const uint64_t * keys);
void ref_pairing_heap_insert(int id);
int ref_pairing_heap_empty();
int ref_pairing_heap_first();
int ref_pairing_heap_any();
int ref_pairing_heap_remove_first();
int ref_pairing_heap_remove_any();
void ref_pairing_heap_remove(int id);
int ref_pairing_heap_enumerate(uint16_t max_visit_num, uint16_t max_queue_size, int * out);
int ref_pairing_heap_root();
size_t ref_pairing_heap_auxiliary_count();
void ref_pairing_heap_links(int id, int * out);
}

namespace
{

constexpr int max_nodes = 4096;
constexpr uint16_t queue_size = 32;

struct Node
{
    uint64_t key;
    int id;
    PairingHeapLink<Node> link;
};

struct NodeCompare
{
    int operator()(const Node * a, const Node * b) const { return (a->key > b->key) - (a->key < b->key); }
};

using Heap = PairingHeap<Node, &Node::link, NodeCompare>;

Node nodes[max_nodes];
Heap heap;
bool member[max_nodes];
int members[max_nodes];
int position[max_nodes];
int num_members = 0;

uint64_t rng_state = 0;

uint64_t nextRandom()
{
    /// splitmix64
    uint64_t z = (rng_state += 0x9e3779b97f4a7c15ull);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ull;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebull;
    return z ^ (z >> 31);
}

uint64_t randomBelow(uint64_t n)
{
    return nextRandom() % n;
}

int idOf(const Node * node)
{
    return node ? node->id : -1;
}

void addMember(int id)
{
    member[id] = true;
    position[id] = num_members;
    members[num_members++] = id;
}

void removeMember(int id)
{
    REQUIRE(member[id]);
    member[id] = false;
    int pos = position[id];
    int last = members[--num_members];
    members[pos] = last;
    position[last] = pos;
}

bool compareStructure(int n)
{
    if (idOf(heap.rootNode()) != ref_pairing_heap_root() || heap.auxiliaryCount() != ref_pairing_heap_auxiliary_count())
    {
        CHECK_EQ(idOf(heap.rootNode()), ref_pairing_heap_root());
        CHECK_EQ(heap.auxiliaryCount(), ref_pairing_heap_auxiliary_count());
        return false;
    }
    for (int i = 0; i < n; ++i)
    {
        int ref[3];
        ref_pairing_heap_links(i, ref);
        if (idOf(nodes[i].link.prev) != ref[0] || idOf(nodes[i].link.next) != ref[1] || idOf(nodes[i].link.left_child) != ref[2])
        {
            std::fprintf(
                stderr,
                "Links of node %d differ: (%d %d %d) vs (%d %d %d)\n",
                i,
                idOf(nodes[i].link.prev),
                idOf(nodes[i].link.next),
                idOf(nodes[i].link.left_child),
                ref[0],
                ref[1],
                ref[2]);
            CHECK(false);
            return false;
        }
    }
    return true;
}

/// Runs a trace; returns false at the first mismatch.
bool runTrace(uint64_t seed, int n, uint64_t key_range, int steps, unsigned insert_weight)
{
    rng_state = seed;
    uint64_t keys[max_nodes];
    for (int i = 0; i < n; ++i)
    {
        keys[i] = randomBelow(key_range);
        nodes[i].key = keys[i];
        nodes[i].id = i;
        member[i] = false;
    }
    num_members = 0;
    heap.init();
    ref_pairing_heap_init(n, keys);

    for (int step = 0; step < steps; ++step)
    {
        unsigned op = static_cast<unsigned>(randomBelow(insert_weight + 7));
        if (op >= 7)
            op = 0;

        int mine = -2;
        int ref = -2;
        switch (op)
        {
            case 0: /// insert
            {
                if (num_members == n)
                    break;
                int id;
                do
                    id = static_cast<int>(randomBelow(n));
                while (member[id]);
                heap.insert(&nodes[id]);
                ref_pairing_heap_insert(id);
                addMember(id);
                break;
            }
            case 1:
                mine = idOf(heap.first());
                ref = ref_pairing_heap_first();
                break;
            case 2:
                mine = idOf(heap.any());
                ref = ref_pairing_heap_any();
                break;
            case 3:
                mine = idOf(heap.removeFirst());
                ref = ref_pairing_heap_remove_first();
                if (mine >= 0 && mine == ref)
                    removeMember(mine);
                break;
            case 4:
                mine = idOf(heap.removeAny());
                ref = ref_pairing_heap_remove_any();
                if (mine >= 0 && mine == ref)
                    removeMember(mine);
                break;
            case 5: /// remove a random member
            {
                if (num_members == 0)
                    break;
                int id = members[randomBelow(num_members)];
                heap.remove(&nodes[id]);
                ref_pairing_heap_remove(id);
                removeMember(id);
                break;
            }
            case 6: /// enumerate
            {
                if (heap.empty() != (ref_pairing_heap_empty() != 0))
                {
                    CHECK(false);
                    return false;
                }
                if (heap.empty())
                    break;
                /// The queue can only overflow (harmlessly) on the last visit when max_queue >= max_visit, see
                /// `enumerateQueuePush`; with smaller queues jemalloc would corrupt the BFS order.
                uint16_t max_visit = static_cast<uint16_t>(1 + randomBelow(queue_size));
                uint16_t max_queue = static_cast<uint16_t>(max_visit + randomBelow(queue_size - max_visit + 1));
                if (randomBelow(2))
                    max_visit = max_queue = queue_size; /// As used by `extent_set`.

                int ref_out[2 * queue_size];
                int ref_count = ref_pairing_heap_enumerate(max_visit, max_queue, ref_out);
                Heap::EnumerateHelper<queue_size> helper;
                heap.enumeratePrepare(helper, max_visit, max_queue);
                int count = 0;
                while (Node * node = heap.enumerateNext(helper))
                {
                    if (count >= ref_count || node->id != ref_out[count])
                    {
                        std::fprintf(stderr, "Enumeration differs at step %d, position %d\n", step, count);
                        CHECK(false);
                        return false;
                    }
                    ++count;
                }
                CHECK_EQ(count, ref_count);
                if (count != ref_count)
                    return false;
                break;
            }
        }

        if (mine != ref)
        {
            std::fprintf(stderr, "Seed %llu step %d op %u: %d vs %d\n", static_cast<unsigned long long>(seed), step, op, mine, ref);
            CHECK(false);
            return false;
        }
        if ((n <= 256 || step % 97 == 0 || step + 1 == steps) && !compareStructure(n))
        {
            std::fprintf(stderr, "Seed %llu step %d op %u: structure differs\n", static_cast<unsigned long long>(seed), step, op);
            return false;
        }
    }
    return true;
}

}

TEST(PairingHeapOracle, SmallManyTies)
{
    for (uint64_t seed = 1; seed <= 200; ++seed)
        if (!runTrace(seed, 16, 4, 2000, 3))
            return;
}

TEST(PairingHeapOracle, MediumTies)
{
    for (uint64_t seed = 1000; seed < 1050; ++seed)
        if (!runTrace(seed, 256, 32, 20000, 4))
            return;
}

TEST(PairingHeapOracle, MediumDistinct)
{
    for (uint64_t seed = 2000; seed < 2050; ++seed)
        if (!runTrace(seed, 256, uint64_t(1) << 40, 20000, 2))
            return;
}

TEST(PairingHeapOracle, LargeInsertHeavy)
{
    for (uint64_t seed = 3000; seed < 3010; ++seed)
        if (!runTrace(seed, max_nodes, 1000, 100000, 12))
            return;
}

TEST(PairingHeapOracle, LargeBalanced)
{
    for (uint64_t seed = 4000; seed < 4010; ++seed)
        if (!runTrace(seed, max_nodes, uint64_t(1) << 20, 100000, 6))
            return;
}
