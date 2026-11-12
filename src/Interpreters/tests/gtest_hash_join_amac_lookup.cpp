#include <gtest/gtest.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/AmacLookup.h>
#include <Interpreters/HashJoin/FindKeys.h>
#include <Interpreters/HashJoin/HashJoinTable.h>
#include <Interpreters/HashJoin/PrepareKeys.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/JoinUtils.h>
#include <Common/PODArray.h>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

using namespace DB;
using namespace DB::JoinProbe;

/// The key search of the AMAC ring (`AmacLookup.h`) must write the words and key offsets that `PrefetchingLookup` writes for
/// the same `FindKeysArgs`. The tests call the two `findKeys` on one frozen table, because a query reaches the ring only
/// with a table larger than the L2 cache. For a key type that builds its keys, the prepare step (`PrepareKeys.h`) runs before
/// the search by the ring, as in the probe.
namespace
{

constexpr size_t fixed_string_size = 40;

/// How the key columns are made from row ids.
enum class KeyShape : uint8_t
{
    Number32,
    Number64,
    String,
    /// Strings of 16 bytes or more, which a packed `String` key holds as a hash.
    LongString,
    /// A `String` and a `UInt64`, the key columns of a `hashed` map.
    StringAndNumber,
    FixedString,
    Packed32,
    Packed64,
    Packed128,
    Packed256,
    /// A `FixedString(3)` and a `UInt8`: `HashMethodKeysFixed` packs widths other than 1, 2, 4, 8 and 16 row by row.
    PackedOddWidths,
};

/// A key type of the one-bucket maps, and key columns for which the join chooses this type.
struct KeyClass
{
    const char * name;
    HashJoin::Type type;
    KeyShape shape;
    Sizes key_sizes;
};

/// The key types that the ring supports. A table of the parallel layout has the `two_level_` type of each.
const std::vector<KeyClass> & keyClasses()
{
    static const std::vector<KeyClass> classes{
        {"key32", HashJoin::Type::key32, KeyShape::Number32, {4}},
        {"key64", HashJoin::Type::key64, KeyShape::Number64, {8}},
        {"key_string", HashJoin::Type::key_string, KeyShape::String, {0}},
        {"key_fixed_string", HashJoin::Type::key_fixed_string, KeyShape::FixedString, {fixed_string_size}},
        {"keys32", HashJoin::Type::keys32, KeyShape::Packed32, {2, 2}},
        {"keys64", HashJoin::Type::keys64, KeyShape::Packed64, {4, 4}},
        {"keys128", HashJoin::Type::keys128, KeyShape::Packed128, {8, 8}},
        {"keys256", HashJoin::Type::keys256, KeyShape::Packed256, {8, 8, 8}},
        {"keys32 of odd widths", HashJoin::Type::keys32, KeyShape::PackedOddWidths, {3, 1}},
        {"key_packed_string", HashJoin::Type::key_packed_string, KeyShape::String, {0}},
        {"key_packed_string of long strings", HashJoin::Type::key_packed_string, KeyShape::LongString, {0}},
        {"hashed", HashJoin::Type::hashed, KeyShape::StringAndNumber, {0, 8}},
    };
    return classes;
}

const KeyClass & keyClass(HashJoin::Type type)
{
    return *std::ranges::find(keyClasses(), type, &KeyClass::type);
}

/// A join kind, strictness and maps kind that select `Maps`.
template <typename Maps>
struct JoinOf;

template <>
struct JoinOf<HashJoin::MapsOne>
{
    static constexpr JoinKind kind = JoinKind::Left;
    static constexpr JoinStrictness strictness = JoinStrictness::Any;
    static constexpr JoinMapsKind maps_kind = JoinMapsKind::Default;
};

template <>
struct JoinOf<HashJoin::MapsAll>
{
    static constexpr JoinKind kind = JoinKind::Inner;
    static constexpr JoinStrictness strictness = JoinStrictness::All;
    static constexpr JoinMapsKind maps_kind = JoinMapsKind::Default;
};

template <>
struct JoinOf<HashJoin::MapsAsof>
{
    static constexpr JoinKind kind = JoinKind::Inner;
    static constexpr JoinStrictness strictness = JoinStrictness::Asof;
    static constexpr JoinMapsKind maps_kind = JoinMapsKind::Default;
};

template <>
struct JoinOf<HashJoin::MapsSet>
{
    static constexpr JoinKind kind = JoinKind::Left;
    static constexpr JoinStrictness strictness = JoinStrictness::Semi;
    static constexpr JoinMapsKind maps_kind = JoinMapsKind::Set;
};

template <typename Maps>
constexpr bool is_asof = std::is_same_v<Maps, HashJoin::MapsAsof>;

/// Only these maps record key offsets.
template <typename Maps>
constexpr bool is_mapped = std::is_same_v<Maps, HashJoin::MapsOne> || std::is_same_v<Maps, HashJoin::MapsAll>;

template <typename T, typename ValueOfId>
ColumnWithTypeAndName numberColumn(const String & name, const std::vector<UInt64> & ids, ValueOfId && value_of_id)
{
    auto column = ColumnVector<T>::create();
    auto & data = column->getData();
    data.reserve(ids.size());
    for (const UInt64 id : ids)
        data.push_back(static_cast<T>(value_of_id(id)));
    return {std::move(column), std::make_shared<DataTypeNumber<T>>(), name};
}

/// The string key of an id. Id 0 has the empty string, which is the zero key of a string map.
std::string stringKeyOf(UInt64 id)
{
    if (id == 0)
        return {};
    std::string key(reinterpret_cast<const char *>(&id), sizeof(id));
    key.append(id % 3, 'x');
    return key;
}

ColumnWithTypeAndName stringColumn(const String & name, const std::vector<UInt64> & ids, bool long_strings)
{
    auto column = ColumnString::create();
    for (const UInt64 id : ids)
    {
        std::string key = stringKeyOf(id);
        if (long_strings && id != 0)
            key.insert(0, "a prefix of 21 bytes ");
        column->insertData(key.data(), key.size());
    }
    return {std::move(column), std::make_shared<DataTypeString>(), name};
}

/// The key columns of the rows `ids`. Equal ids have equal keys. Id 0 has the zero key of the map, except for a fixed string
/// and for the key of a `hashed` map, whose maps have no zero key that a row can have.
ColumnsWithTypeAndName keyColumns(KeyShape shape, const std::vector<UInt64> & ids)
{
    switch (shape)
    {
        case KeyShape::Number32:
            return {numberColumn<UInt32>("k0", ids, [](UInt64 id) { return id; })};
        case KeyShape::Number64:
            return {numberColumn<UInt64>("k0", ids, [](UInt64 id) { return id; })};
        case KeyShape::String:
            return {stringColumn("k0", ids, /*long_strings=*/false)};
        case KeyShape::LongString:
            return {stringColumn("k0", ids, /*long_strings=*/true)};
        case KeyShape::StringAndNumber:
            return {stringColumn("k0", ids, /*long_strings=*/false), numberColumn<UInt64>("k1", ids, [](UInt64 id) { return id * 31; })};
        case KeyShape::FixedString:
        {
            auto column = ColumnFixedString::create(fixed_string_size);
            for (const UInt64 id : ids)
            {
                const std::string key = fmt::format("{:0{}}", id, fixed_string_size);
                column->insertData(key.data(), key.size());
            }
            return {ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeFixedString>(fixed_string_size), "k0")};
        }
        case KeyShape::Packed32:
            return {
                numberColumn<UInt16>("k0", ids, [](UInt64 id) { return id; }),
                numberColumn<UInt16>("k1", ids, [](UInt64 id) { return id >> 16; })};
        case KeyShape::Packed64:
            return {
                numberColumn<UInt32>("k0", ids, [](UInt64 id) { return id; }),
                numberColumn<UInt32>("k1", ids, [](UInt64 id) { return id * 31; })};
        case KeyShape::Packed128:
            return {
                numberColumn<UInt64>("k0", ids, [](UInt64 id) { return id; }),
                numberColumn<UInt64>("k1", ids, [](UInt64 id) { return id * 31; })};
        case KeyShape::Packed256:
            return {
                numberColumn<UInt64>("k0", ids, [](UInt64 id) { return id; }),
                numberColumn<UInt64>("k1", ids, [](UInt64 id) { return id * 31; }),
                numberColumn<UInt64>("k2", ids, [](UInt64 id) { return id * 131; })};
        case KeyShape::PackedOddWidths:
        {
            auto column = ColumnFixedString::create(3);
            for (const UInt64 id : ids)
                column->insertData(reinterpret_cast<const char *>(&id), 3);
            return {
                ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeFixedString>(3), "k0"),
                numberColumn<UInt8>("k1", ids, [](UInt64 id) { return id >> 24; })};
        }
    }
    UNREACHABLE();
}

/// The `ASOF` column of the rows `ids`, the last key column of an `ASOF` join.
ColumnWithTypeAndName asofColumn(const std::vector<UInt64> & ids)
{
    return numberColumn<UInt32>("t", ids, [](UInt64 id) { return id % 100; });
}

/// An empty table of `Maps`. Its map is there already, so a test can ask it for the hash of a key. A two-level map may
/// switch to the Robin Hood mode, as `enable_robin_hood_hash_for_joins` allows.
template <typename Maps>
std::unique_ptr<HashJoinTable> createTable(const KeyClass & key_class, bool two_level)
{
    Sizes key_sizes = key_class.key_sizes;
    if constexpr (is_asof<Maps>)
        key_sizes.push_back(sizeof(UInt32));
    return std::make_unique<HashJoinTable>(
        key_class.type,
        key_sizes,
        JoinOf<Maps>::kind,
        JoinOf<Maps>::strictness,
        JoinOf<Maps>::maps_kind,
        two_level ? HashJoinBuildLayout::twoLevel(1) : HashJoinBuildLayout::oneBucket(1),
        /*robin_hood_allowed=*/true);
}

/// Inserts the keys of the rows `ids` as one right block and freezes the table, as the end of the build does.
template <typename Maps>
void fillAndFreeze(HashJoinTable & table, const KeyClass & key_class, const std::vector<UInt64> & ids)
{
    ColumnsWithTypeAndName columns = keyColumns(key_class.shape, ids);
    if constexpr (is_asof<Maps>)
        columns.push_back(asofColumn(ids));

    ColumnRawPtrs key_columns;
    for (const auto & column : columns)
        key_columns.push_back(column.column.get());
    const ScatteredBlock::Selector selector(0, ids.size());
    const JoinCommon::JoinMask join_mask(true, ids.size());
    const HashJoinTable::InsertSettings settings{
        .any_take_last_row = false,
        .enable_software_prefetch = true,
        .must_keep_right_blocks = false,
        .asof_type = is_asof<Maps> ? std::optional<TypeIndex>(TypeIndex::UInt32) : std::nullopt,
        .asof_inequality = ASOFJoinInequality::GreaterOrEquals};
    table.insert<Maps>(
        settings, {key_columns, selector, /*stored_block_no=*/0, /*null_map=*/nullptr, join_mask}, /*stream=*/0, [](size_t) {});
    table.freezeForProbing();
}

/// The key columns of a left block with the rows `ids`, as the probe prepares them. With `null_rows`, the rows it marks have
/// a NULL key. With `mask`, the ON section has a condition that is false for the rows where `mask` is 0.
template <typename Maps>
std::unique_ptr<JoinOnKeyColumns> makeJoinKeys(
    const KeyClass & key_class, const std::vector<UInt64> & ids, const std::vector<UInt8> & null_rows, const std::vector<UInt8> & mask)
{
    ColumnsWithTypeAndName columns = keyColumns(key_class.shape, ids);
    Sizes key_sizes = key_class.key_sizes;
    if (!null_rows.empty())
    {
        /// The key is NULL if one of its columns is.
        auto null_map = ColumnUInt8::create();
        null_map->getData().assign(null_rows.begin(), null_rows.end());
        const ColumnPtr null_map_column = std::move(null_map);
        auto & first = columns.front();
        first.column = ColumnNullable::create(first.column, null_map_column);
        first.type = std::make_shared<DataTypeNullable>(first.type);
    }
    if constexpr (is_asof<Maps>)
    {
        columns.push_back(asofColumn(ids));
        key_sizes.push_back(sizeof(UInt32));
    }
    Names key_names;
    for (const auto & column : columns)
        key_names.push_back(column.name);

    String condition_name;
    if (!mask.empty())
    {
        auto condition = ColumnUInt8::create();
        condition->getData().assign(mask.begin(), mask.end());
        condition_name = "condition";
        columns.emplace_back(std::move(condition), std::make_shared<DataTypeUInt8>(), condition_name);
    }

    const ScatteredBlock block{Block(columns)};
    return std::make_unique<JoinOnKeyColumns>(block, key_names, condition_name, key_sizes);
}

/// Searches the keys of `rows` with `PrefetchingLookup` and with the ring, and expects the same words and key offsets. The
/// result of block row `ind` goes to the slot `ind - base`, and `found[ind]` tells whether the row has a match.
template <typename Maps>
void expectRingFindsWhatPrefetchingLookupFinds(
    const HashJoinTable & table,
    const JoinOnKeyColumns & keys,
    const std::vector<UInt8> & found,
    ProbeRows rows,
    size_t base,
    FindKeysMode mode,
    bool fast_path,
    bool robin_hood)
{
    const FindKeysFn in_row_order = selectFindKeys<Maps, PrefetchingLookup>(table.type(), mode, fast_path);
    const FindKeysFn ring = robin_hood
        ? selectFindKeys<Maps, AmacLookup<true>>(table.type(), mode, fast_path)
        : selectFindKeys<Maps, AmacLookup<false>>(table.type(), mode, fast_path);
    ASSERT_TRUE(in_row_order != nullptr);
    ASSERT_TRUE(ring != nullptr);

    const auto row_of = [&](size_t i) -> size_t { return rows.rows ? rows.rows[i] : rows.first + i; };
    const size_t slots = rows.size ? row_of(rows.size - 1) + 1 - base : 0;
    const bool records_key_offsets = mode == FindKeysMode::WordsAndKeyOffsets;

    /// A slot that the search does not visit keeps this value.
    constexpr UInt64 not_visited = 0x5A5A5A5A5A5A5A5AULL;
    std::vector<MatchWord> words_in_row_order(slots, not_visited);
    std::vector<MatchWord> words_ring(slots, not_visited);
    std::vector<size_t> key_offsets_in_row_order(records_key_offsets ? slots : 0, not_visited);
    std::vector<size_t> key_offsets_ring(records_key_offsets ? slots : 0, not_visited);

    const auto args = [&](std::vector<MatchWord> & words, std::vector<size_t> & key_offsets) -> FindKeysArgs
    {
        return {
            table,
            keys,
            rows,
            /*enable_prefetch=*/true,
            base,
            words.data(),
            records_key_offsets ? key_offsets.data() : nullptr,
            /*max_output_rows=*/0};
    };
    ASSERT_EQ(in_row_order(args(words_in_row_order, key_offsets_in_row_order)), rows.size);

    const FindKeysArgs ring_args = args(words_ring, key_offsets_ring);
    if (const PrepareKeysFn prepare_keys = selectPrepareKeys<Maps>(table.type(), rows.rows != nullptr, fast_path))
        prepare_keys(ring_args);
    ASSERT_EQ(ring(ring_args), rows.size);

    const auto first_difference = [](const auto & left, const auto & right) -> size_t
    {
        return static_cast<size_t>(std::ranges::mismatch(left, right).in1 - left.begin());
    };
    ASSERT_EQ(first_difference(words_ring, words_in_row_order), slots) << "the first slot whose words differ";
    ASSERT_EQ(first_difference(key_offsets_ring, key_offsets_in_row_order), key_offsets_in_row_order.size())
        << "the first slot whose key offsets differ";

    /// The two lookups could agree on a wrong answer only if the test filled the table or the block wrongly.
    size_t wrong_rows = 0;
    for (size_t i = 0; i < rows.size; ++i)
        wrong_rows += (words_ring[row_of(i) - base] != 0) != (found[row_of(i)] != 0);
    ASSERT_EQ(wrong_rows, 0u) << "rows whose word does not tell whether the table has their key";
}

/// The keys of the tables below: the ids `2 * key_no` for `key_no` in `[1, num_keys]`. An odd id is a key of no table.
constexpr size_t num_keys = 5000;
/// The search ranges start at this block row, so that the slot of a row is not its block row.
constexpr size_t first_row = 7;
constexpr size_t max_rows = 20000;
/// Fewer rows than the ring has slots, the ring size, the minimum of 256 rows that the probe gives the ring, and the chunk
/// of 8192 rows of `AmacLookup`.
constexpr std::array<size_t, 11> row_counts{0, 1, 31, 32, 33, 255, 256, 8191, 8192, 8193, max_rows};

/// The ids of the right rows. Every third key has two rows, so that a `MapsAll` cell holds a row list.
std::vector<UInt64> buildIds(bool with_zero_id)
{
    std::vector<UInt64> ids;
    if (with_zero_id)
        ids.push_back(0);
    for (UInt64 key_no = 1; key_no <= num_keys; ++key_no)
    {
        ids.push_back(2 * key_no);
        if (key_no % 3 == 0)
            ids.push_back(2 * key_no);
    }
    return ids;
}

enum class HitRate : uint8_t
{
    All,
    Half,
    None,
};

/// Whether a row is found, in a block where half of the rows are found. It does not depend on which key `scattered` gives
/// the row.
bool hitsInHalf(size_t row)
{
    return (row / 3) % 2 == 0;
}

/// The index that a row takes out of `size` keys: every key in turn, in a scattered order.
size_t scattered(size_t row, size_t size)
{
    return (row * 7919) % size;
}

/// The ids of the left rows: the keys in a scattered order, or for a row that must miss, the odd id next to a key. One row
/// in 16 has id 0 at any hit rate.
std::vector<UInt64> probeIds(HitRate hit_rate)
{
    std::vector<UInt64> ids(first_row + max_rows);
    for (size_t row = 0; row < ids.size(); ++row)
    {
        if (row % 16 == 8)
            continue;
        const UInt64 key_no = 1 + scattered(row, num_keys);
        const bool hit = hit_rate == HitRate::All || (hit_rate == HitRate::Half && hitsInHalf(row));
        ids[row] = 2 * key_no + !hit;
    }
    return ids;
}

/// Compares the two lookups on a table of `key_class`, in every mode that `Maps` has. With `reduced`, the range searches run
/// one row count at one hit rate, and the searches with NULL keys and with a condition in the ON section are left out; the
/// row-list searches always run every size.
template <typename Maps>
void checkKeyClass(const KeyClass & key_class, bool two_level, bool with_zero_id, bool reduced)
{
    SCOPED_TRACE(fmt::format("{}, two_level {}, with_zero_id {}", key_class.name, two_level, with_zero_id));

    auto table = createTable<Maps>(key_class, two_level);
    fillAndFreeze<Maps>(*table, key_class, buildIds(with_zero_id));
    ASSERT_EQ(table->keys(), num_keys + with_zero_id);

    const auto has_key = [&](UInt64 id) -> bool { return id == 0 ? with_zero_id : id % 2 == 0; };
    const auto range = [](size_t rows) -> ProbeRows { return {.first = first_row, .rows = nullptr, .size = rows}; };

    std::vector<FindKeysMode> modes{FindKeysMode::Words};
    if constexpr (is_mapped<Maps>)
        modes.push_back(FindKeysMode::WordsAndKeyOffsets);

    for (const HitRate hit_rate : {HitRate::Half, HitRate::All, HitRate::None})
    {
        if (reduced && hit_rate != HitRate::Half)
            continue;

        const std::vector<UInt64> ids = probeIds(hit_rate);
        const auto keys = makeJoinKeys<Maps>(key_class, ids, {}, {});
        std::vector<UInt8> found(ids.size());
        for (size_t row = 0; row < ids.size(); ++row)
            found[row] = has_key(ids[row]);

        /// Every row count with half of the rows found, and the largest one at the other hit rates.
        for (const size_t rows : row_counts)
        {
            if (reduced ? rows != 8193 : (hit_rate != HitRate::Half && rows != max_rows))
                continue;
            for (const FindKeysMode mode : modes)
                for (const bool fast_path : {false, true})
                {
                    SCOPED_TRACE(fmt::format("rows {}, mode {}, fast_path {}", rows, static_cast<int>(mode), fast_path));
                    ASSERT_NO_FATAL_FAILURE(expectRingFindsWhatPrefetchingLookupFinds<Maps>(
                        *table, *keys, found, range(rows), first_row, mode, fast_path, false));
                }
        }

        /// A join that stops at the first match gives a later disjunct the rows that are still unmatched. Never an `ASOF` join.
        if constexpr (!is_asof<Maps>)
        {
            if (hit_rate != HitRate::Half)
                continue;
            PaddedPODArray<UInt32> list;
            for (size_t row = first_row + 2; row < ids.size(); row += 2)
                list.push_back(static_cast<UInt32>(row));
            for (const size_t rows : {0uz, 1uz, 33uz, 256uz, 8193uz, list.size()})
                for (const bool fast_path : {false, true})
                {
                    SCOPED_TRACE(fmt::format("row list of {} rows, fast_path {}", rows, fast_path));
                    ASSERT_NO_FATAL_FAILURE(expectRingFindsWhatPrefetchingLookupFinds<Maps>(
                        *table,
                        *keys,
                        found,
                        {.first = 0, .rows = list.data(), .size = rows},
                        first_row,
                        FindKeysMode::WordsOfRowList,
                        fast_path,
                        false));
                }
        }
    }

    if (reduced)
        return;

    /// NULL keys, a condition in the ON section, and both. A NULL key keeps the bytes of its id under the NULL.
    const std::vector<UInt64> ids = probeIds(HitRate::Half);
    std::vector<UInt8> null_rows(ids.size());
    std::vector<UInt8> mask(ids.size());
    for (size_t row = 0; row < ids.size(); ++row)
    {
        null_rows[row] = row % 5 == 0;
        mask[row] = row % 7 != 0;
    }
    for (const bool with_nulls : {false, true})
        for (const bool with_mask : {false, true})
        {
            if (!with_nulls && !with_mask)
                continue;
            const auto keys = makeJoinKeys<Maps>(
                key_class, ids, with_nulls ? null_rows : std::vector<UInt8>{}, with_mask ? mask : std::vector<UInt8>{});
            std::vector<UInt8> found(ids.size());
            for (size_t row = 0; row < ids.size(); ++row)
                found[row] = has_key(ids[row]) && !(with_nulls && null_rows[row]) && !(with_mask && !mask[row]);

            for (const size_t rows : {300uz, max_rows})
                for (const FindKeysMode mode : modes)
                {
                    SCOPED_TRACE(fmt::format(
                        "rows {}, mode {}, with_nulls {}, with_mask {}", rows, static_cast<int>(mode), with_nulls, with_mask));
                    ASSERT_NO_FATAL_FAILURE(expectRingFindsWhatPrefetchingLookupFinds<Maps>(
                        *table, *keys, found, range(rows), first_row, mode, /*fast_path=*/false, false));
                }
        }
}

/// Rows that never enter the ring. A NULL key is answered in `start`, so one refill of a slot passes over a whole run of NULL
/// keys. The runs are as long as the ring or longer: at the first row, in the middle, and in the last rows, where the ring
/// drains. A zero key is answered in `start` too, so a block of only zero keys never puts a lookup into the ring.
template <typename Maps>
void checkRowsOutsideTheRing(const KeyClass & key_class, bool two_level, bool with_zero_id)
{
    SCOPED_TRACE(fmt::format("{}, two_level {}, with_zero_id {}", key_class.name, two_level, with_zero_id));

    auto table = createTable<Maps>(key_class, two_level);
    fillAndFreeze<Maps>(*table, key_class, buildIds(with_zero_id));
    const auto has_key = [&](UInt64 id) -> bool { return id == 0 ? with_zero_id : id % 2 == 0; };
    const auto range = [](size_t rows) -> ProbeRows { return {.first = first_row, .rows = nullptr, .size = rows}; };

    std::vector<FindKeysMode> modes{FindKeysMode::Words};
    if constexpr (is_mapped<Maps>)
        modes.push_back(FindKeysMode::WordsAndKeyOffsets);

    const std::vector<UInt64> ids = probeIds(HitRate::Half);
    std::vector<UInt8> null_rows(ids.size());
    const std::array<std::pair<size_t, size_t>, 4> null_runs{
        {{first_row, 32}, {first_row + 500, 33}, {first_row + 1000, 70}, {ids.size() - 50, 50}}};
    for (const auto & [begin, size] : null_runs)
        std::fill_n(null_rows.begin() + begin, size, 1);
    const auto keys = makeJoinKeys<Maps>(key_class, ids, null_rows, {});
    std::vector<UInt8> found(ids.size());
    for (size_t row = 0; row < ids.size(); ++row)
        found[row] = has_key(ids[row]) && !null_rows[row];
    for (const size_t rows : {300uz, max_rows})
        for (const FindKeysMode mode : modes)
        {
            SCOPED_TRACE(fmt::format("runs of NULL keys, rows {}, mode {}", rows, static_cast<int>(mode)));
            ASSERT_NO_FATAL_FAILURE(expectRingFindsWhatPrefetchingLookupFinds<Maps>(
                *table, *keys, found, range(rows), first_row, mode, /*fast_path=*/false, false));
        }

    const std::vector<UInt64> zero_ids(first_row + 8193, 0);
    const auto zero_keys = makeJoinKeys<Maps>(key_class, zero_ids, {}, {});
    const std::vector<UInt8> zero_found(zero_ids.size(), has_key(0));
    for (const size_t rows : {300uz, 8193uz})
        for (const FindKeysMode mode : modes)
            for (const bool fast_path : {false, true})
            {
                SCOPED_TRACE(fmt::format("only zero keys, rows {}, mode {}, fast_path {}", rows, static_cast<int>(mode), fast_path));
                ASSERT_NO_FATAL_FAILURE(expectRingFindsWhatPrefetchingLookupFinds<Maps>(
                    *table, *zero_keys, zero_found, range(rows), first_row, mode, fast_path, false));
            }
}

template <typename Maps>
class HashJoinAmacLookup : public ::testing::Test
{
};

using AllMaps = ::testing::Types<HashJoin::MapsOne, HashJoin::MapsAll, HashJoin::MapsAsof, HashJoin::MapsSet>;

}

TYPED_TEST_SUITE(HashJoinAmacLookup, AllMaps);


/// Every key type that the ring supports, in both layouts, with the zero key in the table and without it.
TYPED_TEST(HashJoinAmacLookup, FindsWhatPrefetchingLookupFinds)
{
    for (const KeyClass & key_class : keyClasses())
        for (const bool two_level : {false, true})
        {
            ASSERT_NO_FATAL_FAILURE(checkKeyClass<TypeParam>(key_class, two_level, /*with_zero_id=*/true, /*reduced=*/false));
            ASSERT_NO_FATAL_FAILURE(checkKeyClass<TypeParam>(key_class, two_level, /*with_zero_id=*/false, /*reduced=*/true));
        }
}

TYPED_TEST(HashJoinAmacLookup, RowsOutsideTheRing)
{
    for (const HashJoin::Type type :
         {HashJoin::Type::key64, HashJoin::Type::key_string, HashJoin::Type::keys128, HashJoin::Type::key_packed_string})
        for (const bool two_level : {false, true})
            for (const bool with_zero_id : {false, true})
                ASSERT_NO_FATAL_FAILURE(checkRowsOutsideTheRing<TypeParam>(keyClass(type), two_level, with_zero_id));
}


namespace
{

/// A bucket gets keys whose home cells are its last few cells, and more keys than those cells can hold. So their chain
/// continues at the start of the buffer, and present and absent keys with those home cells walk past the end.
/// With `robin_hood`, the table is reserved so that every bucket starts in the Robin Hood mode. There an absent key may be
/// stopped by the Robin Hood rule after the walk wraps, where a cell's distance from its home cell wraps too.
template <HashJoin::Type TYPE>
void checkWalkPastTheEndOfTheBuffer(bool two_level, bool robin_hood)
{
    using Maps = HashJoin::MapsOne;
    using Map = JoinMapOf<TYPE, Maps>;
    const size_t bucket = robin_hood ? 42 : Map::NUM_BUCKETS - 1;
    const size_t cells = robin_hood ? 1uz << 16 : 256;
    const size_t first_home_cell = cells - (robin_hood ? 32uz : 10uz);
    constexpr size_t keys_of_each_kind = 100;

    const KeyClass & key_class = keyClass(HashJoin::Type::key64);
    auto table = createTable<Maps>(key_class, two_level);
    const Map & map = table->template mapOf<TYPE, Maps>();

    std::vector<UInt64> present;
    std::vector<UInt64> absent;
    for (UInt64 id = 1; absent.size() < keys_of_each_kind; ++id)
    {
        const size_t hash = map.hash(id);
        if (Map::getBucketFromHash(hash) != bucket || (hash & (cells - 1)) < first_home_cell)
            continue;
        (present.size() < keys_of_each_kind ? present : absent).push_back(id);
    }
    if (robin_hood)
        table->reserve(Map::NUM_BUCKETS * (cells / 2 + 1));
    fillAndFreeze<Maps>(*table, key_class, present);
    if (robin_hood)
    {
        ASSERT_TRUE(table->bucketsAreRobinHood());
        ASSERT_TRUE(table->takesAmacLookup());
    }

    const auto & bucket_cells = map.bucket_cells[bucket];
    ASSERT_EQ(bucket_cells.mask + 1, cells) << "the cells of bucket " << bucket;
    size_t keys_past_the_end = 0;
    for (const UInt64 id : present)
    {
        const auto * cell = map.find(id);
        ASSERT_NE(cell, nullptr) << "id " << id;
        keys_past_the_end += static_cast<size_t>(cell - bucket_cells.cells) < first_home_cell;
    }
    ASSERT_GT(keys_past_the_end, 0u);

    /// Every present and every absent key, ten times each.
    std::vector<UInt64> ids;
    std::vector<UInt8> found;
    for (size_t row = 0; row < 20 * keys_of_each_kind; ++row)
    {
        const bool hit = row % 2 == 0;
        ids.push_back((hit ? present : absent)[(row / 2) % keys_of_each_kind]);
        found.push_back(hit);
    }
    const auto keys = makeJoinKeys<Maps>(key_class, ids, {}, {});
    for (const FindKeysMode mode : {FindKeysMode::Words, FindKeysMode::WordsAndKeyOffsets})
        for (const bool fast_path : {false, true})
            ASSERT_NO_FATAL_FAILURE(expectRingFindsWhatPrefetchingLookupFinds<Maps>(
                *table, *keys, found, {.first = 0, .rows = nullptr, .size = ids.size()}, 0, mode, fast_path, robin_hood));
}

/// A table of the parallel layout that is reserved for so many keys that every bucket starts in the Robin Hood mode. Four
/// buckets are filled to about 70% with keys chosen by their hash. The left rows have keys of those buckets, half of them
/// absent, so that a miss stops by the Robin Hood rule. The cells of the 256 buckets take about 270 MB for `key64`.
template <HashJoin::Type TYPE, typename Maps>
void checkBucketsInRobinHoodMode()
{
    using Map = JoinMapOf<TYPE, Maps>;
    constexpr size_t cells_per_bucket = 1uz << 16;
    constexpr size_t keys_per_bucket = cells_per_bucket * 7 / 10;
    constexpr size_t absent_keys = 20000;
    constexpr std::array<size_t, 4> filled_buckets{0, 85, 170, 255};
    /// A bucket reserved for more keys than half of 2^16 cells gets 2^16 cells in the Robin Hood mode.
    constexpr size_t keys_to_reserve = Map::NUM_BUCKETS * (cells_per_bucket / 2 + 1);
    constexpr bool string_key = std::is_same_v<typename Map::key_type, std::string_view>;
    constexpr bool packed_string_key = TYPE == HashJoin::Type::two_level_key_packed_string;

    const KeyClass & key_class = keyClass(
        packed_string_key ? HashJoin::Type::key_packed_string : string_key ? HashJoin::Type::key_string : HashJoin::Type::key64);
    auto table = createTable<Maps>(key_class, /*two_level=*/true);
    const Map & map = table->template mapOf<TYPE, Maps>();
    const auto bucket_of = [&](UInt64 id) -> size_t
    {
        if constexpr (packed_string_key)
        {
            /// `PackedStringJoinKey::build` reads 16 bytes from the start of the string.
            const std::string key = stringKeyOf(id);
            std::array<char, 16> padded{};
            std::copy(key.begin(), key.end(), padded.begin());
            return Map::getBucketFromHash(map.hash(PackedStringJoinKey::build(padded.data(), key.size())));
        }
        else if constexpr (string_key)
        {
            const std::string key = stringKeyOf(id);
            return Map::getBucketFromHash(map.hash(std::string_view(key)));
        }
        else
            return Map::getBucketFromHash(map.hash(id));
    };

    /// The first keys of a filled bucket go to the table. The keys after them are absent.
    std::vector<UInt64> present;
    std::vector<UInt64> absent;
    std::array<size_t, Map::NUM_BUCKETS> keys_in_bucket{};
    for (UInt64 id = 1; present.size() < filled_buckets.size() * keys_per_bucket || absent.size() < absent_keys; ++id)
    {
        const size_t bucket = bucket_of(id);
        if (std::ranges::find(filled_buckets, bucket) == filled_buckets.end())
            continue;
        if (keys_in_bucket[bucket] < keys_per_bucket)
        {
            present.push_back(id);
            ++keys_in_bucket[bucket];
        }
        else if (absent.size() < absent_keys)
            absent.push_back(id);
    }

    table->reserve(keys_to_reserve);
    fillAndFreeze<Maps>(*table, key_class, present);
    ASSERT_TRUE(map.robin_hood_buckets == Map::RobinHoodBuckets::All);
    ASSERT_TRUE(table->bucketsAreRobinHood());
    ASSERT_TRUE(table->takesAmacLookup());
    for (const size_t bucket : filled_buckets)
        ASSERT_EQ(map.bucket_cells[bucket].mask + 1, cells_per_bucket) << "bucket " << bucket;

    /// Enough rows to look up most keys of the table. One row in 16 has the zero key, which the table does not have.
    constexpr size_t rows_to_probe = 400000;
    std::vector<UInt64> ids(first_row + rows_to_probe);
    std::vector<UInt8> found(ids.size());
    for (size_t row = 0; row < ids.size(); ++row)
    {
        if (row % 16 == 8)
            continue;
        const bool hit = hitsInHalf(row);
        ids[row] = hit ? present[scattered(row, present.size())] : absent[scattered(row, absent.size())];
        found[row] = hit;
    }
    const auto keys = makeJoinKeys<Maps>(key_class, ids, {}, {});

    std::vector<FindKeysMode> modes{FindKeysMode::Words};
    if constexpr (is_mapped<Maps>)
        modes.push_back(FindKeysMode::WordsAndKeyOffsets);
    for (const size_t rows : {300uz, 8193uz, rows_to_probe})
        for (const FindKeysMode mode : modes)
            for (const bool fast_path : {false, true})
            {
                SCOPED_TRACE(fmt::format("rows {}, mode {}, fast_path {}", rows, static_cast<int>(mode), fast_path));
                ASSERT_NO_FATAL_FAILURE(expectRingFindsWhatPrefetchingLookupFinds<Maps>(
                    *table, *keys, found, {.first = first_row, .rows = nullptr, .size = rows}, first_row, mode, fast_path, true));
            }

    PaddedPODArray<UInt32> list;
    for (size_t row = first_row + 2; row < ids.size(); row += 2)
        list.push_back(static_cast<UInt32>(row));
    ASSERT_NO_FATAL_FAILURE(expectRingFindsWhatPrefetchingLookupFinds<Maps>(
        *table,
        *keys,
        found,
        {.first = 0, .rows = list.data(), .size = list.size()},
        first_row,
        FindKeysMode::WordsOfRowList,
        /*fast_path=*/true,
        true));
}

}


TEST(HashJoinAmacLookupWalk, GoesPastTheEndOfTheBuffer)
{
    ASSERT_NO_FATAL_FAILURE(checkWalkPastTheEndOfTheBuffer<HashJoin::Type::key64>(/*two_level=*/false, /*robin_hood=*/false));
    ASSERT_NO_FATAL_FAILURE(checkWalkPastTheEndOfTheBuffer<HashJoin::Type::two_level_key64>(/*two_level=*/true, /*robin_hood=*/false));
}

TEST(HashJoinAmacLookupWalk, GoesPastTheEndOfTheBufferInRobinHoodMode)
{
    ASSERT_NO_FATAL_FAILURE(checkWalkPastTheEndOfTheBuffer<HashJoin::Type::two_level_key64>(/*two_level=*/true, /*robin_hood=*/true));
}


/// The Robin Hood rule reads the hash of the key in a cell: computed from an integer key, saved in the cell of a string key.
TEST(HashJoinAmacLookupRobinHood, IntegerKeys)
{
    checkBucketsInRobinHoodMode<HashJoin::Type::two_level_key64, HashJoin::MapsOne>();
}

TEST(HashJoinAmacLookupRobinHood, StringKeys)
{
    checkBucketsInRobinHoodMode<HashJoin::Type::two_level_key_string, HashJoin::MapsSet>();
}

/// A key of 128 bits, which the prepare step builds.
TEST(HashJoinAmacLookupRobinHood, PackedStringKeys)
{
    checkBucketsInRobinHoodMode<HashJoin::Type::two_level_key_packed_string, HashJoin::MapsSet>();
}
