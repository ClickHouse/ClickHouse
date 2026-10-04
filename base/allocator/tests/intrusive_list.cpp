/// Tests of the `qr`/`ql`/`typed_list` equivalents: explicit orders and a randomized comparison with an array model.

#include <allocator/IntrusiveList.h>

#include "Test.h"

using namespace jemalloc;

namespace
{

struct Node
{
    int id;
    RingLink<Node> link;
};

using List = IntrusiveList<Node, &Node::link>;
using NodeRing = Ring<Node, &Node::link>;
using NodeTypedList = TypedList<Node, &Node::link>;

constexpr int max_nodes = 64;

/// Writes the forward order to `out` and checks that the reverse order and next/prev/last agree with it.
int collect(const List & list, int * out)
{
    int n = 0;
    for (Node * node : list)
        out[n++] = node->id;

    int m = 0;
    list.forEach([&](Node * node) { CHECK_EQ(node->id, out[m++]); });
    CHECK_EQ(m, n);

    int r = n;
    list.reverseForEach([&](Node * node) { CHECK_EQ(node->id, out[--r]); });
    CHECK_EQ(r, 0);

    if (n == 0)
    {
        CHECK(list.empty());
        CHECK(list.first() == nullptr);
        CHECK(list.last() == nullptr);
    }
    else
    {
        CHECK_EQ(list.first()->id, out[0]);
        CHECK_EQ(list.last()->id, out[n - 1]);
        int i = 0;
        for (Node * node = list.first(); node; node = list.next(node))
            CHECK_EQ(node->id, out[i++]);
        CHECK_EQ(i, n);
        for (Node * node = list.last(); node; node = list.prev(node))
            CHECK_EQ(node->id, out[--i]);
        CHECK_EQ(i, 0);
    }
    return n;
}

bool equalsModel(const List & list, const int * model, int model_size)
{
    int actual[max_nodes];
    int n = collect(list, actual);
    if (n != model_size)
        return false;
    for (int i = 0; i < n; ++i)
        if (actual[i] != model[i])
            return false;
    return true;
}

template <size_t N>
void checkOrder(const List & list, const int (&expected)[N])
{
    CHECK(equalsModel(list, expected, N));
}

void checkEmpty(const List & list)
{
    CHECK(equalsModel(list, nullptr, 0));
}

}

TEST(IntrusiveList, Basic)
{
    Node nodes[5];
    List list;
    checkEmpty(list);
    for (int i = 0; i < 5; ++i)
    {
        nodes[i].id = i;
        List::elementInit(&nodes[i]);
    }

    list.tailInsert(&nodes[0]);
    list.tailInsert(&nodes[1]);
    list.headInsert(&nodes[2]);
    checkOrder(list, {2, 0, 1});

    list.beforeInsert(&nodes[2], &nodes[3]);
    checkOrder(list, {3, 2, 0, 1});
    List::afterInsert(&nodes[1], &nodes[4]);
    checkOrder(list, {3, 2, 0, 1, 4});

    list.rotate();
    checkOrder(list, {2, 0, 1, 4, 3});

    list.remove(&nodes[0]);
    checkOrder(list, {2, 1, 4, 3});
    list.headRemove();
    checkOrder(list, {1, 4, 3});
    list.tailRemove();
    checkOrder(list, {1, 4});
    list.remove(&nodes[1]);
    list.remove(&nodes[4]);
    checkEmpty(list);
}

TEST(IntrusiveList, ConcatSplit)
{
    Node nodes[6];
    List a;
    List b;
    for (int i = 0; i < 6; ++i)
    {
        nodes[i].id = i;
        List::elementInit(&nodes[i]);
        (i < 3 ? a : b).tailInsert(&nodes[i]);
    }
    checkOrder(a, {0, 1, 2});
    checkOrder(b, {3, 4, 5});

    a.concat(b);
    checkOrder(a, {0, 1, 2, 3, 4, 5});
    checkEmpty(b);

    a.split(&nodes[4], b);
    checkOrder(a, {0, 1, 2, 3});
    checkOrder(b, {4, 5});

    b.concat(a);
    checkOrder(b, {4, 5, 0, 1, 2, 3});
    checkEmpty(a);

    /// Concatenating into an empty list moves.
    a.concat(b);
    checkOrder(a, {4, 5, 0, 1, 2, 3});
    checkEmpty(b);

    /// Splitting at the head moves everything.
    a.split(&nodes[4], b);
    checkEmpty(a);
    checkOrder(b, {4, 5, 0, 1, 2, 3});

    a.moveFrom(b);
    checkOrder(a, {4, 5, 0, 1, 2, 3});
    checkEmpty(b);
}

TEST(IntrusiveList, Ring)
{
    Node nodes[4];
    for (int i = 0; i < 4; ++i)
    {
        nodes[i].id = i;
        NodeRing::init(&nodes[i]);
    }
    NodeRing::meld(&nodes[0], &nodes[1]);
    NodeRing::meld(&nodes[2], &nodes[3]);
    NodeRing::meld(&nodes[0], &nodes[2]);

    int order[4];
    int n = 0;
    NodeRing::forEach(&nodes[1], [&](Node * node) { order[n++] = node->id; });
    CHECK_EQ(n, 4);
    CHECK_EQ(order[0], 1);
    CHECK_EQ(order[1], 2);
    CHECK_EQ(order[2], 3);
    CHECK_EQ(order[3], 0);

    n = 0;
    NodeRing::reverseForEach(&nodes[1], [&](Node * node) { order[n++] = node->id; });
    CHECK_EQ(n, 4);
    CHECK_EQ(order[0], 0);
    CHECK_EQ(order[1], 3);
    CHECK_EQ(order[2], 2);
    CHECK_EQ(order[3], 1);

    NodeRing::split(&nodes[0], &nodes[2]);
    CHECK(NodeRing::next(&nodes[1]) == &nodes[0]);
    CHECK(NodeRing::next(&nodes[3]) == &nodes[2]);

    NodeRing::remove(&nodes[3]);
    CHECK(NodeRing::next(&nodes[3]) == &nodes[3]);
    CHECK(NodeRing::next(&nodes[2]) == &nodes[2]);
    CHECK(NodeRing::prev(&nodes[2]) == &nodes[2]);

    n = 0;
    NodeRing::forEach(nullptr, [&](Node *) { ++n; });
    NodeRing::reverseForEach(nullptr, [&](Node *) { ++n; });
    CHECK_EQ(n, 0);
}

TEST(IntrusiveList, TypedList)
{
    Node nodes[5];
    for (int i = 0; i < 5; ++i)
        nodes[i].id = i;

    NodeTypedList list;
    CHECK(list.empty());
    list.append(&nodes[0]);
    list.append(&nodes[1]);
    list.prepend(&nodes[2]);
    checkOrder(list.raw(), {2, 0, 1});
    CHECK_EQ(list.first()->id, 2);
    CHECK_EQ(list.last()->id, 1);
    CHECK_EQ(list.next(&nodes[2])->id, 0);
    CHECK(list.next(&nodes[1]) == nullptr);

    list.replace(&nodes[0], &nodes[3]);
    checkOrder(list.raw(), {2, 3, 1});
    list.replace(&nodes[2], &nodes[4]);
    checkOrder(list.raw(), {4, 3, 1});

    NodeTypedList other;
    other.append(&nodes[0]);
    other.append(&nodes[2]);
    list.concat(other);
    CHECK(other.empty());
    checkOrder(list.raw(), {4, 3, 1, 0, 2});

    list.remove(&nodes[1]);
    checkOrder(list.raw(), {4, 3, 0, 2});

    int n = 0;
    for (Node * node : list)
        n += node->id;
    CHECK_EQ(n, 9);
}

TEST(IntrusiveList, RandomizedModel)
{
    Node nodes[max_nodes];
    /// Which list (0, 1) each node is in, or -1.
    int where[max_nodes];
    List lists[2];
    int model[2][max_nodes];
    int model_size[2] = {0, 0};

    for (int i = 0; i < max_nodes; ++i)
    {
        nodes[i].id = i;
        where[i] = -1;
    }

    auto model_index = [&](int l, int id)
    {
        for (int i = 0; i < model_size[l]; ++i)
            if (model[l][i] == id)
                return i;
        return -1;
    };
    auto model_insert = [&](int l, int pos, int id)
    {
        for (int i = model_size[l]; i > pos; --i)
            model[l][i] = model[l][i - 1];
        model[l][pos] = id;
        ++model_size[l];
        where[id] = l;
    };
    auto model_erase = [&](int l, int pos)
    {
        where[model[l][pos]] = -1;
        for (int i = pos; i + 1 < model_size[l]; ++i)
            model[l][i] = model[l][i + 1];
        --model_size[l];
    };

    uint64_t rng = 1;
    auto random_below = [&](uint64_t n)
    {
        rng = rng * 6364136223846793005ull + 1442695040888963407ull;
        return (rng >> 33) % n;
    };

    for (int step = 0; step < 200000; ++step)
    {
        int l = static_cast<int>(random_below(2));
        List & list = lists[l];
        unsigned op = static_cast<unsigned>(random_below(10));

        int free_id = -1;
        for (int attempt = 0; attempt < 4 && free_id < 0; ++attempt)
        {
            int id = static_cast<int>(random_below(max_nodes));
            if (where[id] < 0)
                free_id = id;
        }
        int member_pos = model_size[l] ? static_cast<int>(random_below(model_size[l])) : -1;
        Node * member = member_pos >= 0 ? &nodes[model[l][member_pos]] : nullptr;

        switch (op)
        {
            case 0:
                if (free_id < 0)
                    break;
                List::elementInit(&nodes[free_id]);
                list.headInsert(&nodes[free_id]);
                model_insert(l, 0, free_id);
                break;
            case 1:
                if (free_id < 0)
                    break;
                List::elementInit(&nodes[free_id]);
                list.tailInsert(&nodes[free_id]);
                model_insert(l, model_size[l], free_id);
                break;
            case 2:
                if (free_id < 0 || !member)
                    break;
                List::elementInit(&nodes[free_id]);
                list.beforeInsert(member, &nodes[free_id]);
                model_insert(l, member_pos, free_id);
                break;
            case 3:
                if (free_id < 0 || !member)
                    break;
                List::elementInit(&nodes[free_id]);
                List::afterInsert(member, &nodes[free_id]);
                model_insert(l, member_pos + 1, free_id);
                break;
            case 4:
            case 5:
                if (!member)
                    break;
                list.remove(member);
                model_erase(l, model_index(l, member->id));
                break;
            case 6:
                if (list.empty())
                    break;
                if (random_below(2))
                {
                    list.headRemove();
                    model_erase(l, 0);
                }
                else
                {
                    list.tailRemove();
                    model_erase(l, model_size[l] - 1);
                }
                break;
            case 7:
                if (list.empty())
                    break;
                list.rotate();
                {
                    int first = model[l][0];
                    model_erase(l, 0);
                    model_insert(l, model_size[l], first);
                }
                break;
            case 8: /// concat the other list into this one
            {
                int o = 1 - l;
                list.concat(lists[o]);
                for (int i = 0; i < model_size[o]; ++i)
                    model_insert(l, model_size[l], model[o][i]);
                model_size[o] = 0;
                break;
            }
            case 9: /// split this list into the other one, which must be empty
            {
                int o = 1 - l;
                if (!member || !lists[o].empty())
                    break;
                list.split(member, lists[o]);
                for (int i = member_pos; i < model_size[l]; ++i)
                {
                    model[o][model_size[o]++] = model[l][i];
                    where[model[l][i]] = o;
                }
                model_size[l] = member_pos;
                break;
            }
        }

        if (!equalsModel(lists[0], model[0], model_size[0]) || !equalsModel(lists[1], model[1], model_size[1]))
        {
            std::fprintf(stderr, "Mismatch at step %d, op %u\n", step, op);
            CHECK(false);
            return;
        }
    }
}
