#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>
#include <Common/ColumnsHashing.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/PODArray.h>

#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{

/// arrayTreePreorder(ids, parent_ids) - lays out a forest, given by node ids and parent ids, in depth-first preorder.
/// Returns a tuple of four arrays, all in preorder, with 1-based positions:
///   order[k]  - the input index of the k-th node of the preorder;
///   size[k]   - the size of its subtree, including the node, so the subtree is the range [k, k + size[k]);
///   parent[k] - the preorder position of its parent, or 0 if no node has the parent id;
///   depth[k]  - the distance to the root of its tree.
/// Any input gives a valid forest: orphans become roots, a cycle is cut at the node of the cycle which comes first in the input.
class FunctionArrayTreePreorder final : public IFunction
{
public:
    static constexpr auto name = "arrayTreePreorder";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayTreePreorder>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (size_t i = 0; i < 2; ++i)
            if (!isArray(arguments[i]))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} of function {} must be an array, got {}", i + 1, getName(), arguments[i]->getName());

        /// Check that the elements are comparable.
        getCommonNestedType(arguments);

        auto array_uint32 = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>());
        return std::make_shared<DataTypeTuple>(
            DataTypes{array_uint32, array_uint32, array_uint32, array_uint32}, Names{"order", "size", "parent", "depth"});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        auto common_type = std::make_shared<DataTypeArray>(getCommonNestedType({arguments[0].type, arguments[1].type}));

        ColumnPtr ids_holder = castColumn(arguments[0], common_type)->convertToFullColumnIfConst();
        ColumnPtr parents_holder = castColumn(arguments[1], common_type)->convertToFullColumnIfConst();

        const auto & ids = assert_cast<const ColumnArray &>(*ids_holder);
        const auto & parents = assert_cast<const ColumnArray &>(*parents_holder);

        const auto & offsets = ids.getOffsets();
        if (offsets != parents.getOffsets())
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Arrays passed to {} must have equal sizes", getName());

        const IColumn * ids_data = &ids.getData();
        const IColumn * parents_data = &parents.getData();
        const NullMap * ids_null_map = nullptr;
        const NullMap * parents_null_map = nullptr;

        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(ids_data))
        {
            ids_data = &nullable->getNestedColumn();
            ids_null_map = &nullable->getNullMapData();
        }
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(parents_data))
        {
            parents_data = &nullable->getNestedColumn();
            parents_null_map = &nullable->getNullMapData();
        }

        Result result(ids_data->size());
        Input input{offsets, *ids_data, *parents_data, ids_null_map, parents_null_map};

        if (checkAndGetColumn<ColumnString>(ids_data))
            execute<MethodString>(input, {}, result);
        else if (checkAndGetColumn<ColumnFixedString>(ids_data))
            execute<MethodFixedString>(input, {}, result);
        else if (ids_data->isFixedAndContiguous() && ids_data->sizeOfValueIfFixed() <= sizeof(UInt128))
            execute<MethodFixed>(input, {ids_data->sizeOfValueIfFixed()}, result);
        else
            execute<MethodHashed>(input, {}, result);

        return ColumnTuple::create(Columns{
            ColumnArray::create(std::move(result.order), ids.getOffsetsPtr()),
            ColumnArray::create(std::move(result.size), ids.getOffsetsPtr()),
            ColumnArray::create(std::move(result.parent), ids.getOffsetsPtr()),
            ColumnArray::create(std::move(result.depth), ids.getOffsetsPtr())});
    }

private:
    /// Marks a node without a parent in the input.
    static constexpr UInt32 NO_PARENT = std::numeric_limits<UInt32>::max();

    /// Initially allocate a piece of memory for 64 elements.
    static constexpr size_t INITIAL_SIZE_DEGREE = 6;

    struct MethodString
    {
        using Map = ClearableHashMapWithStackMemory<std::string_view, UInt32, StringViewHash, INITIAL_SIZE_DEGREE>;
        using Method = ColumnsHashing::HashMethodString<typename Map::value_type, UInt32, false, false>;
    };

    struct MethodFixedString
    {
        using Map = ClearableHashMapWithStackMemory<std::string_view, UInt32, StringViewHash, INITIAL_SIZE_DEGREE>;
        using Method = ColumnsHashing::HashMethodFixedString<typename Map::value_type, UInt32, false, false>;
    };

    struct MethodFixed
    {
        using Map = ClearableHashMapWithStackMemory<UInt128, UInt32, UInt128HashCRC32, INITIAL_SIZE_DEGREE>;
        using Method = ColumnsHashing::HashMethodKeysFixed<typename Map::value_type, UInt128, UInt32, false, false, false>;
    };

    struct MethodHashed
    {
        using Map = ClearableHashMapWithStackMemory<UInt128, UInt32, UInt128TrivialHash, INITIAL_SIZE_DEGREE>;
        using Method = ColumnsHashing::HashMethodHashed<typename Map::value_type, UInt32, false>;
    };

    struct Input
    {
        const ColumnArray::Offsets & offsets;
        const IColumn & ids_data;
        const IColumn & parents_data;
        const NullMap * ids_null_map;
        const NullMap * parents_null_map;
    };

    struct Result
    {
        explicit Result(size_t total_size)
            : order(ColumnUInt32::create(total_size))
            , size(ColumnUInt32::create(total_size))
            , parent(ColumnUInt32::create(total_size))
            , depth(ColumnUInt32::create(total_size))
        {
        }

        ColumnUInt32::MutablePtr order;
        ColumnUInt32::MutablePtr size;
        ColumnUInt32::MutablePtr parent;
        ColumnUInt32::MutablePtr depth;
    };

    /// Buffers for one tree, indexed by the 0-based input index. Reused between rows.
    struct Buffers
    {
        PaddedPODArray<UInt32> input_parent;    /// Input index of the parent, or NO_PARENT.
        PaddedPODArray<UInt32> walk;            /// Cycle detection: the number of the walk which visited the node, 0 if none.
        PaddedPODArray<UInt8> is_root;
        PaddedPODArray<UInt32> child_offsets;   /// Children of node v are children[child_offsets[v], child_offsets[v + 1]),
        PaddedPODArray<UInt32> children;        /// the roots are the children of the virtual node n.
        PaddedPODArray<UInt32> position;        /// 0-based preorder position.
        PaddedPODArray<UInt32> stack;
    };

    DataTypePtr getCommonNestedType(const DataTypes & arguments) const
    {
        return getLeastSupertype(DataTypes{
            assert_cast<const DataTypeArray &>(*arguments[0]).getNestedType(),
            assert_cast<const DataTypeArray &>(*arguments[1]).getNestedType()});
    }

    template <typename Method>
    void execute(const Input & input, const Sizes & key_sizes, Result & result) const
    {
        typename Method::Map positions;
        typename Method::Method ids_method({&input.ids_data}, key_sizes, nullptr);
        typename Method::Method parents_method({&input.parents_data}, key_sizes, nullptr);
        Arena pool; /// Not used by these methods.
        Buffers buffers;

        ColumnArray::Offset prev_off = 0;
        for (auto off : input.offsets)
        {
            const size_t n = off - prev_off;
            if (n > std::numeric_limits<UInt32>::max())
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                    "Array of size {} is too large for function {}, the maximum is {}", n, getName(), std::numeric_limits<UInt32>::max());

            positions.clear();
            for (size_t j = prev_off; j < off; ++j)
            {
                if (input.ids_null_map && (*input.ids_null_map)[j])
                    continue;

                /// Children are attached to the first occurrence of an id. Positions are stored 1-based, 0 means a new cell.
                auto emplace_result = ids_method.emplaceKey(positions, j, pool);
                if (!emplace_result.getMapped())
                    emplace_result.setMapped(static_cast<UInt32>(j - prev_off + 1));
            }

            buffers.input_parent.resize(n);
            for (size_t j = prev_off; j < off; ++j)
            {
                UInt32 & parent = buffers.input_parent[j - prev_off];
                parent = NO_PARENT;

                if (input.parents_null_map && (*input.parents_null_map)[j])
                    continue;

                auto find_result = parents_method.findKey(positions, j, pool);
                if (find_result.isFound())
                    parent = find_result.getMapped() - 1;
            }

            layoutTree(n, prev_off, buffers, result);
            prev_off = off;
        }
    }

    static void layoutTree(size_t n, size_t offset, Buffers & buffers, Result & result)
    {
        const auto & input_parent = buffers.input_parent;

        /// Find the roots. Walking up from any node ends either at a node without a parent or in a cycle.
        /// Each cycle is cut at its node which comes first in the input, and this node becomes a root.
        /// Every node is walked at most once: a walk stops at a node visited by any previous walk.
        buffers.is_root.resize(n);
        buffers.walk.assign(n, static_cast<UInt32>(0));
        for (size_t i = 0; i < n; ++i)
            buffers.is_root[i] = input_parent[i] == NO_PARENT;

        for (size_t start = 0; start < n; ++start)
        {
            const UInt32 walk = static_cast<UInt32>(start + 1);
            UInt32 v = static_cast<UInt32>(start);
            while (v != NO_PARENT && buffers.walk[v] == 0)
            {
                buffers.walk[v] = walk;
                v = input_parent[v];
            }

            /// The walk came back to a node visited by itself: v is on a new cycle.
            if (v != NO_PARENT && buffers.walk[v] == walk)
            {
                UInt32 first = v;
                for (UInt32 u = input_parent[v]; u != v; u = input_parent[u])
                    first = std::min(first, u);
                buffers.is_root[first] = true;
            }
        }

        /// Children lists, keeping the input order.
        auto tree_parent = [&](size_t i) { return buffers.is_root[i] ? n : input_parent[i]; };

        buffers.child_offsets.assign(n + 2, static_cast<UInt32>(0));
        for (size_t i = 0; i < n; ++i)
            ++buffers.child_offsets[tree_parent(i) + 1];
        for (size_t v = 0; v <= n; ++v)
            buffers.child_offsets[v + 1] += buffers.child_offsets[v];

        buffers.children.resize(n);
        buffers.stack.assign(buffers.child_offsets.begin(), buffers.child_offsets.end() - 1); /// Used as the fill cursors.
        for (size_t i = 0; i < n; ++i)
            buffers.children[buffers.stack[tree_parent(i)]++] = static_cast<UInt32>(i);

        UInt32 * order = &result.order->getData()[offset];
        UInt32 * size = &result.size->getData()[offset];
        UInt32 * parent = &result.parent->getData()[offset];
        UInt32 * depth = &result.depth->getData()[offset];

        /// Iterative DFS from the virtual root. The stack holds pairs (node, depth).
        /// Children are pushed in reverse order to be visited in the input order.
        auto push_children = [&](size_t v, UInt32 child_depth)
        {
            for (size_t c = buffers.child_offsets[v + 1]; c > buffers.child_offsets[v]; --c)
            {
                buffers.stack.push_back(buffers.children[c - 1]);
                buffers.stack.push_back(child_depth);
            }
        };

        buffers.position.resize(n);
        buffers.stack.clear();
        push_children(n, 0);

        size_t k = 0;
        while (!buffers.stack.empty())
        {
            const UInt32 node_depth = buffers.stack.back();
            buffers.stack.pop_back();
            const UInt32 v = buffers.stack.back();
            buffers.stack.pop_back();

            buffers.position[v] = static_cast<UInt32>(k);
            order[k] = v + 1;
            depth[k] = node_depth;
            ++k;

            push_children(v, node_depth + 1);
        }

        chassert(k == n);

        /// The parent positions are known only after the DFS: the real parent of a cut cycle node goes after it.
        for (k = 0; k < n; ++k)
        {
            const UInt32 p = input_parent[order[k] - 1];
            parent[k] = p == NO_PARENT ? 0 : buffers.position[p] + 1;
        }

        /// Subtree sizes: accumulate in reverse preorder, so each node is final before it is added to its parent.
        /// For a node which is not a root, parent[k] is its parent in the forest.
        for (k = 0; k < n; ++k)
            size[k] = 1;
        for (k = n; k > 0; --k)
            if (depth[k - 1] != 0)
                size[parent[k - 1] - 1] += size[k - 1];
    }
};

}

REGISTER_FUNCTION(ArrayTreePreorder)
{
    FunctionDocumentation::Description description = R"(
Lays out a tree (or a forest) stored as two arrays, node identifiers and parent identifiers, in depth-first preorder.

Element `i` of `ids` and `parent_ids` describes input node `i` and its parent.
The function returns a named tuple of four arrays. All of them are in preorder, and all positions are 1-based:
element `k` describes the `k`-th node of the preorder.
- `order[k]` — the input index of this node. Data arrays are brought into preorder with `arrayMap(i -> x[i], t.order)`.
- `size[k]` — the number of nodes in the subtree of this node, including the node itself. The subtree is the contiguous range of positions `[k, k + size[k])`.
- `parent[k]` — the preorder position of the node whose identifier equals the parent identifier of this node, or `0` if there is no such node.
- `depth[k]` — the distance from this node to the root of its tree, `0` for the roots.

Node `a` is an ancestor of node `b` if and only if `a < b AND b < a + size[a]`.
Additive aggregates over a subtree are the difference of two prefix sums (see [`arrayCumSum`](#arrayCumSum)).
Other aggregates over a subtree are available with [`arraySlice`](#arraySlice) of a data array reordered into preorder.

The trees are written one by one, in the input order of their roots, and the children of each node are visited in the input order,
so the result is deterministic. The time complexity is linear in the array size, and the depth of the tree is not limited.

The function does not throw on the content of the arrays, and makes a valid forest from any input:
- A node is a root if its parent identifier is `NULL` or is not present in `ids`, for example an empty string or an orphan node.
- If the parent links make a cycle, the link of the node of the cycle which comes first in the input is cut, and this node becomes a root. Its `parent` keeps the position of its real parent, which is greater than its own position, so `parent[k] > k` shows that a cycle was cut at position `k`. A node which is its own parent gets `parent[k] = k`.
- If `ids` has duplicate values, the children are attached to the first occurrence.

The structure of the input follows from the result (see the examples below):
- Is there a cycle: `arrayExists((p, k) -> p > k, t.parent, arrayEnumerate(t.parent))`.
- Positions where the cycles were cut: `arrayFilter((k, p) -> p > k, arrayEnumerate(t.parent), t.parent)`.
- Positions of the tree roots: `arrayFilter((k, d) -> d = 0, arrayEnumerate(t.depth), t.depth)`, and the number of trees is `countEqual(t.depth, 0)`.
- Positions of the nodes whose parent identifier is not present: `arrayFilter((k, p) -> p = 0, arrayEnumerate(t.parent), t.parent)`.
- Is the input one tree with a real root: `t.size[1] = length(ids) AND t.parent[1] <= 1`. It accepts both root conventions (a parent identifier which is not present gives `0`, a self-loop gives `1`), and rejects an array which is one big cycle.
- Positions of the nodes of the cycle cut at position `r`: `arrayFilter(x -> x + t.size[x] > t.parent[r], range(r, t.parent[r] + 1))`. These are the nodes from `r` down to its real parent `t.parent[r]` whose subtree contains the real parent.
- Duplicate identifiers: `arrayEnumerateUniq(ids) > 1`.

To get the input index of the node at position `k`, use `t.order[k]`.
)";
    FunctionDocumentation::Syntax syntax = "arrayTreePreorder(ids, parent_ids)";
    FunctionDocumentation::Arguments arguments = {
        {"ids", "Node identifiers.", {"Array(T)"}},
        {"parent_ids", "Parent identifiers, one per node. Must have the same size as `ids`.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the tuple of the preorder layout.",
        {"Tuple(order Array(UInt32), size Array(UInt32), parent Array(UInt32), depth Array(UInt32))"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
SELECT arrayTreePreorder(['G2', 'A', 'T1', 'B', 'G1', 'G3'], ['B', '', 'B', 'A', 'A', 'A']) AS t
)", "([2,4,1,3,5,6],[6,3,1,1,1,1],[0,1,2,2,1,1],[0,1,2,2,1,1])"},
        {"Cost and number of descendants of each agent span of a trace", R"(
WITH
    ['A', 'G1', 'B', 'G2', 'T1', 'G3'] AS ids,
    ['', 'A', 'A', 'B', 'B', 'A'] AS parent_ids,
    ['AGENT', 'GENERATION', 'AGENT', 'GENERATION', 'TOOL', 'GENERATION'] AS types,
    [0, 2, 0, 3, 0, 1] AS costs,
    arrayTreePreorder(ids, parent_ids) AS t,
    arrayCumSum(arrayMap(i -> costs[i], t.order)) AS cum
SELECT arrayMap(k -> (ids[t.order[k]], cum[k + t.size[k] - 1] - cum[k], t.size[k] - 1),
    arrayFilter(k -> types[t.order[k]] = 'AGENT', arrayEnumerate(t.order))) AS res
)", "[('A',6,5),('B',3,2)]"},
        {"Invalid input: a cycle C -> D -> E -> C, a node F below the cycle, an orphan G", R"(
SELECT arrayTreePreorder(['A', 'B', 'C', 'D', 'E', 'F', 'G'], ['', 'A', 'D', 'E', 'C', 'C', 'Z']) AS t
)", "([1,2,3,5,4,6,7],[2,1,4,2,1,1,1],[0,1,5,3,4,3,0],[0,1,0,1,2,1,0])"},
        {"Detect cycles and find where they were cut", R"(
WITH
    ['A', 'B', 'C', 'D', 'E', 'F', 'G'] AS ids,
    arrayTreePreorder(ids, ['', 'A', 'D', 'E', 'C', 'C', 'Z']) AS t
SELECT
    arrayExists((p, k) -> p > k, t.parent, arrayEnumerate(t.parent)) AS has_cycle,
    arrayFilter((k, p) -> p > k, arrayEnumerate(t.parent), t.parent) AS cut_positions,
    arrayMap(k -> ids[t.order[k]], cut_positions) AS cut_ids
)", R"(
┌─has_cycle─┬─cut_positions─┬─cut_ids─┐
│         1 │ [3]           │ ['C']   │
└───────────┴───────────────┴─────────┘
)"},
        {"Roots, number of trees, nodes whose parent is not found, check for one tree", R"(
WITH
    ['A', 'B', 'C', 'D', 'E', 'F', 'G'] AS ids,
    arrayTreePreorder(ids, ['', 'A', 'D', 'E', 'C', 'C', 'Z']) AS t
SELECT
    arrayMap(k -> ids[t.order[k]], arrayFilter((k, d) -> d = 0, arrayEnumerate(t.depth), t.depth)) AS roots,
    countEqual(t.depth, 0) AS trees,
    arrayMap(k -> ids[t.order[k]], arrayFilter((k, p) -> p = 0, arrayEnumerate(t.parent), t.parent)) AS parent_not_found,
    t.size[1] = length(ids) AND t.parent[1] <= 1 AS is_one_tree
)", R"(
┌─roots─────────┬─trees─┬─parent_not_found─┬─is_one_tree─┐
│ ['A','C','G'] │     3 │ ['A','G']        │           0 │
└───────────────┴───────┴──────────────────┴─────────────┘
)"},
        {"Nodes of the cycle cut at position 3", R"(
WITH
    ['A', 'B', 'C', 'D', 'E', 'F', 'G'] AS ids,
    arrayTreePreorder(ids, ['', 'A', 'D', 'E', 'C', 'C', 'Z']) AS t,
    3 AS r
SELECT arrayMap(k -> ids[t.order[k]], arrayFilter(x -> x + t.size[x] > t.parent[r], range(r, t.parent[r] + 1))) AS cycle
)", R"(
┌─cycle─────────┐
│ ['C','E','D'] │
└───────────────┘
)"},
        {"Duplicate identifiers: children are attached to the first occurrence", R"(
WITH ['a', 'b', 'a', 'c'] AS ids
SELECT
    arrayFilter((id, n) -> n > 1, ids, arrayEnumerateUniq(ids)) AS duplicate_ids,
    arrayTreePreorder(ids, ['', 'a', '', 'a']).order AS order
)", R"(
┌─duplicate_ids─┬─order─────┐
│ ['a']         │ [1,2,4,3] │
└───────────────┴───────────┘
)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayTreePreorder>(documentation);
}

}
