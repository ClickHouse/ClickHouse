#pragma once

#include <Common/PODArray.h>
#include <base/constexpr_helpers.h>
#include <base/defines.h>
#include <base/types.h>

#include <string_view>
#include <type_traits>
#include <utility>


namespace DB
{

/// Whether an aggregation table iterates its cells, which all have one type, as `HashTable` does.
template <typename Table>
concept IterableCells = requires (Table & table) { table.begin(); };

/// Whether an aggregation table keeps its cells in submaps, each with its own type of cell, as the string
/// table does. Such a table gives the number of its submaps and the type of the cells of each. It walks its
/// cells submap by submap with their hashes (see `StringHashTable::forEachCellWithHash`), and turns a cell
/// into the key that its iterations hand out.
template <typename Table>
concept SubmapCells = requires (Table & table)
{
    Table::NUM_SUBMAPS;
    table.forEachCellWithHash([](auto, auto &, size_t) {});
    Table::template iteratedKey<0>(std::declval<const typename Table::template SubmapCell<0> &>());
};

/// How a cell index sees the cells of an aggregation table (see `AggregationCellIndex`): in key classes, each
/// with its own type of cell. Each kind of table has its own specialization. It provides the number of
/// classes `key_classes`, the type `Cell<key_class>` of the cells of a class, `forEachCellWithHash` and
/// `iteratedKey`. A table of any other kind does not compile.
template <typename Table>
struct CellIndexTraits;

/// A table that iterates its cells has one key class.
template <IterableCells Table>
struct CellIndexTraits<Table>
{
    static constexpr size_t key_classes = 1;

    template <size_t key_class>
    using Cell = typename Table::cell_type;

    /// Calls `func(key_class, cell, hash)` for every cell of `table`, key class by key class: `key_class` is
    /// a `std::integral_constant`, and `hash` is the hash of the cell's key.
    template <typename Func>
    static void ALWAYS_INLINE forEachCellWithHash(Table & table, Func && func)
    {
        for (auto it = table.begin(); it != table.end(); ++it)
            func(std::integral_constant<size_t, 0>{}, *it, it.getHash());
    }

    /// The key of a cell as `forEachCellWithHash` hands it out, which goes into another table with
    /// `emplaceIteratedKey`.
    template <size_t key_class>
    static decltype(auto) ALWAYS_INLINE iteratedKey(const Cell<key_class> & cell)
    {
        return Cell<key_class>::getKey(cell.getValue());
    }
};

/// A table of submaps has a key class per submap.
template <SubmapCells Table>
struct CellIndexTraits<Table>
{
    static constexpr size_t key_classes = Table::NUM_SUBMAPS;

    template <size_t key_class>
    using Cell = typename Table::template SubmapCell<key_class>;

    template <typename Func>
    static void ALWAYS_INLINE forEachCellWithHash(Table & table, Func && func)
    {
        table.forEachCellWithHash(std::forward<Func>(func));
    }

    template <size_t key_class>
    static std::string_view ALWAYS_INLINE iteratedKey(const Cell<key_class> & cell)
    {
        return Table::template iteratedKey<key_class>(cell);
    }
};

/// Whether the cells of an index built from a `Table` can be read as the cells of a `ReadTable` (see
/// `AggregationCellIndex::forEachCell`): the two have the same key classes, each with cells of the same type.
template <typename Table, typename ReadTable>
constexpr bool cellsReadableAs()
{
    using From = CellIndexTraits<Table>;
    using As = CellIndexTraits<ReadTable>;
    if constexpr (From::key_classes != As::key_classes)
        return false;
    else
        return []<size_t... key_class>(std::index_sequence<key_class...>)
        {
            return (std::is_same_v<typename From::template Cell<key_class>, typename As::template Cell<key_class>> && ...);
        }(std::make_index_sequence<From::key_classes>{});
}

/// The cells of a single-level aggregation table grouped by their key classes (see `CellIndexTraits`), and
/// the cells of a class by partitions of their hashes: a merge reads the cells of any run of consecutive
/// partitions without walking the table, a key class at a time with the code of its type of cell. The index
/// keeps only the partition numbers, and whoever chose the partitions maps them back: the adaptive
/// aggregation indexes by the partitions of its layout, which nest in the buckets of the two-level
/// counterpart, so its merge finds the cells of a bucket by `AdaptivePartitionLayout::firstPartition`. The
/// index points into the table's buffer, which an insertion may move, so it serves only while the table takes
/// no more keys.
class AggregationCellIndex
{
public:
    /// Indexes the cells of `table` by `partition_of` of their hashes with a counting sort. The caller
    /// guarantees that `partition_of` returns a partition below `num_partitions_`. Each hash is computed
    /// once: a table that does not save the hashes computes one for every call of `getHash`, which reads the
    /// bytes of a long string key. The table of a `Nullable` key keeps the null key outside its cells, though
    /// `size` counts it, so the index takes the cells that the table visits; the merges take the null key
    /// from the table.
    template <typename Table, typename PartitionOf>
    AggregationCellIndex(Table & table, size_t num_partitions_, PartitionOf && partition_of);

    size_t numPartitions() const { return num_partitions; }

    /// The cells of the partitions from `first_partition` to `end_partition`.
    size_t cellCount(size_t first_partition, size_t end_partition) const
    {
        size_t count = 0;
        for (size_t key_class = 0; key_class < key_classes; ++key_class)
        {
            const UInt64 * class_offsets = offsets.data() + key_class * num_partitions;
            count += class_offsets[end_partition] - class_offsets[first_partition];
        }
        return count;
    }

    /// Calls `callback(key, mapped, hash)` for each cell of the partitions from `first_partition` to
    /// `end_partition`: key class by key class, and within a class partition by partition, in the order of
    /// the table. So the string table hands out its cells submap by submap, as
    /// `StringHashMap::forEachValueWithHash` does. `key` goes into another table with `emplaceIteratedKey`,
    /// and `mapped` is the cell's slot of the states, or a `VoidMapped` placeholder for a set method. `Table`
    /// is the type of the indexed table or of a bucket of its two-level counterpart, whose cells have the
    /// same types.
    template <typename Table, typename Callback>
    void forEachCell(size_t first_partition, size_t end_partition, Callback && callback) const;

private:
    size_t key_classes;
    size_t num_partitions;
    PaddedPODArray<void *> cells;
    PaddedPODArray<UInt64> hashes;
    /// The cells of key class `c` in partition `p` and their hashes are at the positions from
    /// `offsets[c * num_partitions + p]` to `offsets[c * num_partitions + p + 1]`, so the cells of a class in
    /// a run of partitions take a run of positions.
    PaddedPODArray<UInt64> offsets;
};

template <typename Table, typename PartitionOf>
AggregationCellIndex::AggregationCellIndex(Table & table, size_t num_partitions_, PartitionOf && partition_of)
    : key_classes(CellIndexTraits<Table>::key_classes)
    , num_partitions(num_partitions_)
{
    /// The build reads the number of key classes from the traits: a constant, which folds the arithmetic of a
    /// table with one class.
    using Traits = CellIndexTraits<Table>;
    PaddedPODArray<void *> table_cells(table.size());
    PaddedPODArray<UInt64> table_hashes(table.size());
    PaddedPODArray<UInt64> next;
    next.resize_fill(Traits::key_classes * num_partitions, 0);
    size_t num_cells = 0;
    size_t previous_key_class = 0;
    const auto add_cell = [&](size_t key_class, void * cell, UInt64 hash)
    {
        /// The placement of the cells below relies on the table handing them out key class by key class.
        chassert(key_class >= previous_key_class);
        previous_key_class = key_class;
        const size_t partition = partition_of(hash);
        chassert(partition < num_partitions);
        table_cells[num_cells] = cell;
        table_hashes[num_cells] = hash;
        ++num_cells;
        ++next[key_class * num_partitions + partition];
    };
    Traits::forEachCellWithHash(table, [&](auto key_class, auto & cell, size_t hash) { add_cell(key_class, &cell, hash); });

    offsets.resize(Traits::key_classes * num_partitions + 1);
    UInt64 position = 0;
    for (size_t i = 0; i < Traits::key_classes * num_partitions; ++i)
    {
        offsets[i] = position;
        position += std::exchange(next[i], position);
    }
    offsets[Traits::key_classes * num_partitions] = position;

    /// The table hands out its cells key class by key class (see `CellIndexTraits::forEachCellWithHash`), so
    /// the cells of a class take the same positions in the order of the table as in the index.
    cells.resize(num_cells);
    hashes.resize(num_cells);
    for (size_t key_class = 0; key_class < Traits::key_classes; ++key_class)
    {
        UInt64 * class_next = next.data() + key_class * num_partitions;
        const size_t class_end = offsets[(key_class + 1) * num_partitions];
        for (size_t cell = offsets[key_class * num_partitions]; cell < class_end; ++cell)
        {
            const UInt64 i = class_next[partition_of(table_hashes[cell])]++;
            cells[i] = table_cells[cell];
            hashes[i] = table_hashes[cell];
        }
    }
}

template <typename Table, typename Callback>
void AggregationCellIndex::forEachCell(size_t first_partition, size_t end_partition, Callback && callback) const
{
    /// The cells are cast to the types of the cells of `Table`, so it has the key classes of the indexed
    /// table.
    using Traits = CellIndexTraits<Table>;
    chassert(key_classes == Traits::key_classes);
    static_for<size_t{0}, Traits::key_classes>([&](auto class_constant)
    {
        constexpr size_t key_class = decltype(class_constant)::value;
        using Cell = typename Traits::template Cell<key_class>;
        const UInt64 * class_offsets = offsets.data() + key_class * num_partitions;
        for (size_t i = class_offsets[first_partition]; i < class_offsets[end_partition]; ++i)
        {
            Cell & cell = *static_cast<Cell *>(cells[i]);
            callback(Traits::template iteratedKey<key_class>(cell), cell.getMapped(), hashes[i]);
        }
    });
}

}
