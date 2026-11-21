#include <Interpreters/ContextTimeSeriesTagsCollector.h>

#include <Common/PODArray.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitors.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/SharedLockGuard.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <base/unaligned.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <utility>
#include <city.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

namespace
{
    using TagNamesAndValues = ContextTimeSeriesTagsCollector::TagNamesAndValues;
    using TagNamesAndValuesPtr = ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr;
    using Group = ContextTimeSeriesTagsCollector::Group;

    const Group INVALID_GROUP = static_cast<Group>(-1);

    [[noreturn]] void throwGroupOutOfBound(Group group, size_t num_groups)
    {
        if (num_groups > 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Group {} is out of bounds, must be between 0 and {}", group, num_groups - 1);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No groups exist");
    }

    [[noreturn]] void throwIDWasAddedWithOtherTags(const IColumn & id_column, size_t row, const TagNamesAndValuesPtr & tags, const TagNamesAndValuesPtr & existing_tags)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Cannot add identifier {} with tags {} because it was added before with tags {}",
                        applyVisitor(FieldVisitorToString{}, id_column[row]),
                        ContextTimeSeriesTagsCollector::toString(*tags),
                        ContextTimeSeriesTagsCollector::toString(*existing_tags));
    }

    [[noreturn]] void throwUnknownID(const IColumn & id_column, size_t row)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown identifier {}", applyVisitor(FieldVisitorToString{}, id_column[row]));
    }

    /// Represents an identifier column with Const/Sparse/LowCardinality/Nullable wrappers removed.
    struct UnwrappedIDColumn
    {
        ColumnPtr column;                   /// The full column, it keeps `data` and `null_map` alive.
        const IColumn * data = nullptr;     /// The column containing values of identifiers (not Nullable).
        const NullMap * null_map = nullptr; /// The null map if the original column was Nullable.
    };

    UnwrappedIDColumn unwrapIDColumn(const ColumnPtr & id_column)
    {
        UnwrappedIDColumn res;
        res.column = id_column->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();
        res.data = res.column.get();
        res.null_map = nullptr;
        if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(res.data))
        {
            res.null_map = &nullable_column->getNullMapData();
            res.data = &nullable_column->getNestedColumn();
        }
        return res;
    }

    /// Converts an element of an identifier column to its representation in a typed map.
    UInt64 toIDComponent(UInt64 x) { return x; }
    UInt128 toIDComponent(const UInt128 & x) { return x; }
    UInt128 toIDComponent(const UUID & x) { return x.toUnderType(); }

    /// Gets identifiers stored in a single column.
    template <typename Type>
    struct OneComponentID
    {
        using IDType = decltype(toIDComponent(Type{}));
        const Type * data;
        [[nodiscard]] IDType get(size_t i) const { return toIDComponent(data[i]); }
    };

    /// Gets identifiers stored in a `FixedString(16)` column, which holds them as raw bytes
    /// without a separate element type, so they are read with the same representation as UInt128.
    struct FixedString16ID
    {
        using IDType = UInt128;
        const UInt8 * data;
        [[nodiscard]] IDType get(size_t i) const { return unalignedLoad<UInt128>(data + sizeof(UInt128) * i); }
    };

    /// Gets identifiers stored in a two-element tuple column by combining two component getters.
    template <typename FirstGetter, typename SecondGetter>
    struct TwoComponentID
    {
        using IDType = std::pair<typename FirstGetter::IDType, typename SecondGetter::IDType>;
        FirstGetter first;
        SecondGetter second;
        [[nodiscard]] IDType get(size_t i) const { return {first.get(i), second.get(i)}; }
    };

    /// Gets identifier components stored in a LowCardinality column: the per-row dictionary index
    /// addresses the getter of the dictionary column, producing the same native representation
    /// as a plain column of the dictionary type.
    template <typename IndexType, typename DictionaryGetter>
    struct LowCardinalityID
    {
        using IDType = typename DictionaryGetter::IDType;
        const IndexType * indexes;
        DictionaryGetter dictionary;
        [[nodiscard]] IDType get(size_t i) const { return dictionary.get(indexes[i]); }
    };

    /// Calls `func` with the raw data of a LowCardinality index column.
    template <typename F>
    void dispatchLowCardinalityIndexes(const IColumn & indexes, F && func)
    {
        if (const auto * indexes_uint8 = typeid_cast<const ColumnUInt8 *>(&indexes))
            func(indexes_uint8->getData().data());
        else if (const auto * indexes_uint16 = typeid_cast<const ColumnUInt16 *>(&indexes))
            func(indexes_uint16->getData().data());
        else if (const auto * indexes_uint32 = typeid_cast<const ColumnUInt32 *>(&indexes))
            func(indexes_uint32->getData().data());
        else if (const auto * indexes_uint64 = typeid_cast<const ColumnUInt64 *>(&indexes))
            func(indexes_uint64->getData().data());
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected index column {} of a LowCardinality column", indexes.getName());
    }

    /// Calls `func` with the typed id getter matching the column's layout and returns true,
    /// or returns false if there is no typed map for this column's layout (the generic map must be used).
    template <typename F>
    bool dispatchIDType(const IColumn & id_data, F && func)
    {
        if (const auto * column_uint64 = typeid_cast<const ColumnUInt64 *>(&id_data))
        {
            func(OneComponentID<UInt64>{column_uint64->getData().data()});
            return true;
        }
        if (const auto * column_uint128 = typeid_cast<const ColumnUInt128 *>(&id_data))
        {
            func(OneComponentID<UInt128>{column_uint128->getData().data()});
            return true;
        }
        if (const auto * column_uuid = typeid_cast<const ColumnUUID *>(&id_data))
        {
            func(OneComponentID<UUID>{column_uuid->getData().data()});
            return true;
        }
        if (const auto * column_fixed_string = typeid_cast<const ColumnFixedString *>(&id_data))
        {
            if (column_fixed_string->getN() == sizeof(UInt128))
            {
                func(FixedString16ID{column_fixed_string->getChars().data()});
                return true;
            }
            return false;
        }

        const auto * column_tuple = typeid_cast<const ColumnTuple *>(&id_data);
        if (!column_tuple || (column_tuple->tupleSize() != 2))
            return false;

        const auto * first = typeid_cast<const ColumnUInt64 *>(&column_tuple->getColumn(0));
        if (!first)
            return false;
        OneComponentID<UInt64> first_getter{first->getData().data()};

        const IColumn & second_column = column_tuple->getColumn(1);
        if (const auto * second_uint64 = typeid_cast<const ColumnUInt64 *>(&second_column))
        {
            func(TwoComponentID{first_getter, OneComponentID<UInt64>{second_uint64->getData().data()}});
            return true;
        }
        if (const auto * second_uint128 = typeid_cast<const ColumnUInt128 *>(&second_column))
        {
            func(TwoComponentID{first_getter, OneComponentID<UInt128>{second_uint128->getData().data()}});
            return true;
        }
        if (const auto * second_uuid = typeid_cast<const ColumnUUID *>(&second_column))
        {
            func(TwoComponentID{first_getter, OneComponentID<UUID>{second_uuid->getData().data()}});
            return true;
        }
        if (const auto * second_fixed_string = typeid_cast<const ColumnFixedString *>(&second_column))
        {
            if (second_fixed_string->getN() == sizeof(UInt128))
            {
                func(TwoComponentID{first_getter, FixedString16ID{second_fixed_string->getChars().data()}});
                return true;
            }
            return false;
        }

        /// A dictionary-encoded (LowCardinality) second component is read through its dictionary:
        /// the getter produces the same native representation as a plain column of the dictionary
        /// type, so these identifiers share their typed map with the plain layout (the store side
        /// receives materialized columns and picks that same map).
        if (const auto * second_low_cardinality = typeid_cast<const ColumnLowCardinality *>(&second_column))
        {
            if (second_low_cardinality->nestedIsNullable())
                return false;

            const IColumn & dictionary = *second_low_cardinality->getDictionary().getNestedColumn();
            bool dispatched = false;
            dispatchLowCardinalityIndexes(second_low_cardinality->getIndexes(), [&](const auto * index_data)
            {
                if (const auto * dictionary_uint64 = typeid_cast<const ColumnUInt64 *>(&dictionary))
                {
                    func(TwoComponentID{first_getter, LowCardinalityID{index_data, OneComponentID<UInt64>{dictionary_uint64->getData().data()}}});
                    dispatched = true;
                }
                else if (const auto * dictionary_uint128 = typeid_cast<const ColumnUInt128 *>(&dictionary))
                {
                    func(TwoComponentID{first_getter, LowCardinalityID{index_data, OneComponentID<UInt128>{dictionary_uint128->getData().data()}}});
                    dispatched = true;
                }
                else if (const auto * dictionary_uuid = typeid_cast<const ColumnUUID *>(&dictionary))
                {
                    func(TwoComponentID{first_getter, LowCardinalityID{index_data, OneComponentID<UUID>{dictionary_uuid->getData().data()}}});
                    dispatched = true;
                }
                else if (const auto * dictionary_fixed_string = typeid_cast<const ColumnFixedString *>(&dictionary))
                {
                    if (dictionary_fixed_string->getN() == sizeof(UInt128))
                    {
                        func(TwoComponentID{first_getter, LowCardinalityID{index_data, FixedString16ID{dictionary_fixed_string->getChars().data()}}});
                        dispatched = true;
                    }
                }
            });
            return dispatched;
        }

        return false;
    }

    /// For an id layout the typed dispatch does not handle, returns the column with any remaining
    /// dictionary encoding materialized (e.g. a LowCardinality first tuple component), to retry
    /// the dispatch with: the choice between a typed map and the generic map must match the store
    /// side, which receives materialized columns. Returns null if there is nothing to materialize.
    ColumnPtr tryMaterializeUnhandledLowCardinalityID(const IColumn & id_data)
    {
        ColumnPtr materialized = recursiveRemoveLowCardinality(id_data.getPtr());
        if (materialized.get() == &id_data)
            return nullptr;
        return materialized;
    }

    /// Serializes identifiers from a column to be used as keys in the mapping.
    /// Identifiers which are NULLs are skipped (their keys are left empty).
    VectorWithMemoryTracking<std::string_view> serializeIDs(const IColumn & id_data, const UInt8 * null_map, Arena & arena)
    {
        size_t num_rows = id_data.size();
        VectorWithMemoryTracking<std::string_view> keys;
        keys.resize(num_rows);
        for (size_t i = 0; i != num_rows; ++i)
        {
            if (null_map && null_map[i])
                continue;
            const char * begin = nullptr;
            keys[i] = id_data.serializeValueIntoArena(i, arena, begin, /* settings = */ nullptr);
        }
        return keys;
    }
}


String ContextTimeSeriesTagsCollector::toString(const TagNamesAndValues & tags)
{
    WriteBufferFromOwnString ostr;
    ostr << "{";
    for (size_t i = 0; i != tags.size(); ++i)
    {
        if (i)
            ostr << ", ";
        ostr << quoteString(tags[i].first) << ": " << quoteString(tags[i].second);
    }
    ostr << "}";
    return ostr.str();
}

String ContextTimeSeriesTagsCollector::toString(const TagNamesAndValuesPtr & tags)
{
    return toString(*tags);
}


ContextTimeSeriesTagsCollector::TagsKey::TagsKey(TagNamesAndValuesPtr tags_)
    : tags(std::move(tags_))
{
    hash = 0;
    for (const auto & [tag_name, tag_value] : *tags)
    {
        hash = CityHash_v1_0_2::CityHash64WithSeed(tag_name.data(), tag_name.length(), hash);
        hash = CityHash_v1_0_2::CityHash64WithSeed(tag_value.data(), tag_value.length(), hash);
    }
}


bool ContextTimeSeriesTagsCollector::Equal::operator()(const TagsKey & left, const TagsKey & right) const
{
    return *left.tags == *right.tags;
}


ContextTimeSeriesTagsCollector::ContextTimeSeriesTagsCollector()
{
    /// Group #0 is reserved for an empty set of tags.
    auto no_tags = std::make_shared<TagNamesAndValues>();
    auto group = tryAddGroupUnlocked(no_tags);
    chassert(group == getGroupForNoTags());
}


ContextTimeSeriesTagsCollector::~ContextTimeSeriesTagsCollector() = default;


Group ContextTimeSeriesTagsCollector::getGroupForTags(const TagNamesAndValuesPtr & tags)
{
    TagsKey key{tags};
    {
        SharedLockGuard lock{mutex};
        auto it = groups_for_tags.find(key);
        if (it != groups_for_tags.end())
            return it->second;
    }

    {
        std::lock_guard lock{mutex};
        return tryAddGroupUnlocked(std::move(key));
    }
}


VectorWithMemoryTracking<Group> ContextTimeSeriesTagsCollector::getGroupForTags(const VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector)
{
    VectorWithMemoryTracking<Group> res;
    res.resize(tags_vector.size(), INVALID_GROUP);
    size_t num_found = 0;

    std::vector<TagsKey> keys;
    keys.reserve(tags_vector.size());
    for (const auto & tags : tags_vector)
        keys.emplace_back(tags);

    {
        SharedLockGuard lock{mutex};
        for (size_t i = 0; i != tags_vector.size(); ++i)
        {
            auto it = groups_for_tags.find(keys[i]);
            if (it != groups_for_tags.end())
            {
                res[i] = it->second;
                ++num_found;
            }
        }
    }

    if (num_found != tags_vector.size())
    {
        std::lock_guard lock{mutex};
        for (size_t i = 0; i != tags_vector.size(); ++i)
        {
            if (res[i] != INVALID_GROUP)
                continue;
            res[i] = tryAddGroupUnlocked(std::move(keys[i]));
            if (++num_found == tags_vector.size())
                break;
        }
    }

    return res;
}


Group ContextTimeSeriesTagsCollector::tryAddGroupUnlocked(TagsKey && key)
{
    UInt64 hash = key.hash;
    TagNamesAndValuesPtr tags = key.tags;
    auto [it, inserted] = groups_for_tags.try_emplace(std::move(key), groups.size());
    if (inserted)
    {
        groups.push_back(std::move(tags));
        sampling_keys.push_back(hash);
    }
    return it->second;
}


Group ContextTimeSeriesTagsCollector::tryAddGroupUnlocked(const TagNamesAndValuesPtr & tags)
{
    return tryAddGroupUnlocked(TagsKey{tags});
}


TagNamesAndValuesPtr ContextTimeSeriesTagsCollector::getTagsByGroup(Group group) const
{
    SharedLockGuard lock{mutex};
    if (group >= groups.size())
        throwGroupOutOfBound(group, groups.size());
    return groups[group];
}


VectorWithMemoryTracking<TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsByGroup(const VectorWithMemoryTracking<Group> & groups_) const
{
    VectorWithMemoryTracking<TagNamesAndValuesPtr> res;
    res.resize(groups_.size());
    SharedLockGuard lock{mutex};
    for (size_t i = 0; i != groups_.size(); ++i)
    {
        Group group = groups_[i];
        if (group >= groups.size())
            throwGroupOutOfBound(group, groups.size());
        res[i] = groups[group];
    }
    return res;
}


UInt64 ContextTimeSeriesTagsCollector::getSamplingKeyByGroup(Group group) const
{
    SharedLockGuard lock{mutex};
    if (group >= sampling_keys.size())
        throwGroupOutOfBound(group, sampling_keys.size());
    return sampling_keys[group];
}


void ContextTimeSeriesTagsCollector::getSamplingKeyByGroup(const VectorWithMemoryTracking<Group> & groups_, PaddedPODArray<UInt64> & res) const
{
    res.resize(groups_.size());
    SharedLockGuard lock{mutex};
    for (size_t i = 0; i != groups_.size(); ++i)
    {
        Group group = groups_[i];
        if (group >= sampling_keys.size())
            throwGroupOutOfBound(group, sampling_keys.size());
        res[i] = sampling_keys[group];
    }
}


template <typename IDType>
ContextTimeSeriesTagsCollector::IDMap<IDType> & ContextTimeSeriesTagsCollector::getTypedIDMap()
{
    if constexpr (std::is_same_v<IDType, UInt64>)
        return id_map_uint64;
    else if constexpr (std::is_same_v<IDType, UInt128>)
        return id_map_uint128;
    else if constexpr (std::is_same_v<IDType, std::pair<UInt64, UInt64>>)
        return id_map_pair_uint64_uint64;
    else
    {
        static_assert(std::is_same_v<IDType, std::pair<UInt64, UInt128>>);
        return id_map_pair_uint64_uint128;
    }
}


template <typename IDType>
const ContextTimeSeriesTagsCollector::IDMap<IDType> & ContextTimeSeriesTagsCollector::getTypedIDMap() const
{
    if constexpr (std::is_same_v<IDType, UInt64>)
        return id_map_uint64;
    else if constexpr (std::is_same_v<IDType, UInt128>)
        return id_map_uint128;
    else if constexpr (std::is_same_v<IDType, std::pair<UInt64, UInt64>>)
        return id_map_pair_uint64_uint64;
    else
    {
        static_assert(std::is_same_v<IDType, std::pair<UInt64, UInt128>>);
        return id_map_pair_uint64_uint128;
    }
}


void ContextTimeSeriesTagsCollector::storeTags(const ColumnPtr & id_column, const VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector)
{
    auto unwrapped = unwrapIDColumn(id_column);
    const NullMap * null_map = unwrapped.null_map;
    size_t num_rows = unwrapped.data->size();
    chassert(num_rows == tags_vector.size());

    size_t num_rows_to_store = num_rows;
    if (null_map)
        num_rows_to_store -= countBytesInFilter(*null_map);
    if (!num_rows_to_store)
        return;

    const UInt8 * null_map_data = null_map ? null_map->data() : nullptr;

    bool dispatched = dispatchIDType(*unwrapped.data, [&](const auto & id_getter)
    {
        storeTagsTyped(id_getter, *unwrapped.data, null_map_data, num_rows_to_store, tags_vector);
    });
    if (dispatched)
        return;

    if (ColumnPtr materialized = tryMaterializeUnhandledLowCardinalityID(*unwrapped.data))
    {
        dispatched = dispatchIDType(*materialized, [&](const auto & id_getter)
        {
            storeTagsTyped(id_getter, *materialized, null_map_data, num_rows_to_store, tags_vector);
        });
        if (!dispatched)
            storeTagsGeneric(*materialized, null_map_data, num_rows_to_store, tags_vector);
        return;
    }

    storeTagsGeneric(*unwrapped.data, null_map_data, num_rows_to_store, tags_vector);
}


template <typename IDGetter>
void ContextTimeSeriesTagsCollector::storeTagsTyped(
    const IDGetter & id_getter,
    const IColumn & id_data,
    const UInt8 * null_map,
    size_t num_rows_to_store,
    const VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector)
{
    using IDType = typename IDGetter::IDType;

    size_t num_rows = tags_vector.size();

    VectorWithMemoryTracking<Group> found_groups;
    found_groups.resize(num_rows, INVALID_GROUP);
    size_t num_found_groups = 0;

    /// Look up the ids which are already stored, under a shared lock.
    {
        SharedLockGuard lock{mutex};
        /// The cast selects the const overload of getTypedIDMap, which requires only a shared lock.
        const auto & id_map = const_cast<const ContextTimeSeriesTagsCollector *>(this)->getTypedIDMap<IDType>().map;

        for (size_t i = 0; i != num_rows; ++i)
        {
            if (null_map && null_map[i])
                continue;

            if (const auto * it = id_map.find(id_getter.get(i)))
            {
                Group existing_group = it->getMapped();
                if (*tags_vector[i] != *groups.at(existing_group))
                    throwIDWasAddedWithOtherTags(id_data, i, tags_vector[i], groups.at(existing_group));
                found_groups[i] = existing_group;
                ++num_found_groups;
            }
        }
    }

    if (num_found_groups == num_rows_to_store)
        return;

    {
        std::lock_guard lock{mutex};
        auto & id_map = getTypedIDMap<IDType>().map;

        for (size_t i = 0; i != num_rows; ++i)
        {
            if (null_map && null_map[i])
                continue;

            if (found_groups[i] != INVALID_GROUP)
                continue;

            Group group = tryAddGroupUnlocked(tags_vector[i]);

            typename HashMap<IDType, Group, IDMapHash>::LookupResult it = nullptr;
            bool inserted = false;
            id_map.emplace(id_getter.get(i), it, inserted);

            if (inserted)
                it->getMapped() = group;
            else if (it->getMapped() != group)
                throwIDWasAddedWithOtherTags(id_data, i, tags_vector[i], groups.at(it->getMapped()));
        }
    }
}


void ContextTimeSeriesTagsCollector::storeTagsGeneric(
    const IColumn & id_data,
    const UInt8 * null_map,
    size_t num_rows_to_store,
    const VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector)
{
    size_t num_rows = tags_vector.size();

    Arena temp_arena;
    auto ids = serializeIDs(id_data, null_map, temp_arena);

    VectorWithMemoryTracking<Group> found_groups;
    found_groups.resize(num_rows, INVALID_GROUP);
    size_t num_found_groups = 0;

    /// Look up the ids which are already stored, under a shared lock.
    {
        SharedLockGuard lock{mutex};

        for (size_t i = 0; i != num_rows; ++i)
        {
            const auto & id = ids[i];
            if (!id.empty())
            {
                if (const auto * it = generic_id_map.map.find(id))
                {
                    Group existing_group = it->getMapped();
                    if (*tags_vector[i] != *groups.at(existing_group))
                        throwIDWasAddedWithOtherTags(id_data, i, tags_vector[i], groups.at(existing_group));
                    found_groups[i] = existing_group;
                    ++num_found_groups;
                }
            }
        }
    }

    if (num_found_groups == num_rows_to_store)
        return;

    {
        std::lock_guard lock{mutex};

        for (size_t i = 0; i != num_rows; ++i)
        {
            const auto id = ids[i];
            if (id.empty())
                continue;

            if (found_groups[i] != INVALID_GROUP)
                continue;

            Group group = tryAddGroupUnlocked(tags_vector[i]);

            GenericIDMap::Map::LookupResult it = nullptr;
            bool inserted = false;
            generic_id_map.map.emplace(ArenaKeyHolder{id, generic_id_map.arena}, it, inserted);

            if (inserted)
                it->getMapped() = group;
            else if (it->getMapped() != group)
                throwIDWasAddedWithOtherTags(id_data, i, tags_vector[i], groups.at(it->getMapped()));
        }
    }
}


void ContextTimeSeriesTagsCollector::getGroupByID(const ColumnPtr & id_column, PaddedPODArray<Group> & res) const
{
    auto unwrapped = unwrapIDColumn(id_column);
    if (unwrapped.null_map)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected Nullable column {} of identifiers", id_column->getName());
    size_t num_rows = unwrapped.data->size();

    bool dispatched = dispatchIDType(*unwrapped.data, [&](const auto & id_getter)
    {
        getGroupByIDTyped(id_getter, *unwrapped.data, num_rows, res);
    });
    if (dispatched)
        return;

    if (ColumnPtr materialized = tryMaterializeUnhandledLowCardinalityID(*unwrapped.data))
    {
        dispatched = dispatchIDType(*materialized, [&](const auto & id_getter)
        {
            getGroupByIDTyped(id_getter, *materialized, num_rows, res);
        });
        if (!dispatched)
            getGroupByIDGeneric(*materialized, num_rows, res);
        return;
    }

    getGroupByIDGeneric(*unwrapped.data, num_rows, res);
}


template <typename IDGetter>
void ContextTimeSeriesTagsCollector::getGroupByIDTyped(
    const IDGetter & id_getter, const IColumn & id_data, size_t num_rows, PaddedPODArray<Group> & res) const
{
    using IDType = typename IDGetter::IDType;

    res.resize(num_rows);
    Group * __restrict out = res.data();

    SharedLockGuard lock{mutex};
    const auto & id_map = getTypedIDMap<IDType>().map;

    /// Id columns arrive in long runs of equal values (samples are sorted by id), so reuse the previous row's group.
    IDType prev_id{};
    Group prev_group = INVALID_GROUP;
    for (size_t i = 0; i != num_rows; ++i)
    {
        IDType id = id_getter.get(i);
        if ((id == prev_id) && (prev_group != INVALID_GROUP))
        {
            out[i] = prev_group;
            continue;
        }
        const auto * it = id_map.find(id);
        if (!it)
            throwUnknownID(id_data, i);
        prev_id = id;
        prev_group = it->getMapped();
        out[i] = prev_group;
    }
}


void ContextTimeSeriesTagsCollector::getGroupByIDGeneric(const IColumn & id_data, size_t num_rows, PaddedPODArray<Group> & res) const
{
    Arena temp_arena;

    res.resize(num_rows);
    Group * __restrict out = res.data();

    SharedLockGuard lock{mutex};

    /// Id columns arrive in long runs of equal values (samples are sorted by id), so reuse the previous row's group.
    Group prev_group = INVALID_GROUP;
    for (size_t i = 0; i != num_rows; ++i)
    {
        if ((i > 0) && id_data.compareAt(i, i - 1, id_data, /* nan_direction_hint = */ 1) == 0)
        {
            chassert(prev_group != INVALID_GROUP);
            out[i] = prev_group;
            continue;
        }
        const char * begin = nullptr;
        auto id = id_data.serializeValueIntoArena(i, temp_arena, begin, /* settings = */ nullptr);
        const auto * it = generic_id_map.map.find(id);
        if (!it)
            throwUnknownID(id_data, i);
        prev_group = it->getMapped();
        out[i] = prev_group;
    }
}


VectorWithMemoryTracking<TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsByID(const ColumnPtr & id_column) const
{
    auto unwrapped = unwrapIDColumn(id_column);
    if (unwrapped.null_map)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected Nullable column {} of identifiers", id_column->getName());
    size_t num_rows = unwrapped.data->size();

    VectorWithMemoryTracking<TagNamesAndValuesPtr> res;
    bool dispatched = dispatchIDType(*unwrapped.data, [&](const auto & id_getter)
    {
        res = getTagsByIDTyped(id_getter, *unwrapped.data, num_rows);
    });
    if (dispatched)
        return res;

    if (ColumnPtr materialized = tryMaterializeUnhandledLowCardinalityID(*unwrapped.data))
    {
        dispatched = dispatchIDType(*materialized, [&](const auto & id_getter)
        {
            res = getTagsByIDTyped(id_getter, *materialized, num_rows);
        });
        if (dispatched)
            return res;

        return getTagsByIDGeneric(*materialized, num_rows);
    }

    return getTagsByIDGeneric(*unwrapped.data, num_rows);
}


template <typename IDGetter>
VectorWithMemoryTracking<TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsByIDTyped(
    const IDGetter & id_getter, const IColumn & id_data, size_t num_rows) const
{
    using IDType = typename IDGetter::IDType;

    VectorWithMemoryTracking<TagNamesAndValuesPtr> res;
    res.reserve(num_rows);

    SharedLockGuard lock{mutex};
    const auto & id_map = getTypedIDMap<IDType>().map;

    /// Id columns arrive in long runs of equal values (samples are sorted by id), so reuse the previous row's tags.
    IDType prev_id{};
    TagNamesAndValuesPtr prev_tags;
    for (size_t i = 0; i != num_rows; ++i)
    {
        IDType id = id_getter.get(i);
        if ((id == prev_id) && prev_tags)
        {
            res.push_back(prev_tags);
            continue;
        }
        const auto * it = id_map.find(id);
        if (!it)
            throwUnknownID(id_data, i);
        prev_id = id;
        prev_tags = groups[it->getMapped()];
        res.push_back(prev_tags);
    }

    return res;
}


VectorWithMemoryTracking<TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsByIDGeneric(
    const IColumn & id_data, size_t num_rows) const
{
    Arena temp_arena;
    auto keys = serializeIDs(id_data, nullptr, temp_arena);

    VectorWithMemoryTracking<TagNamesAndValuesPtr> res;
    res.reserve(num_rows);

    SharedLockGuard lock{mutex};

    for (size_t i = 0; i != num_rows; ++i)
    {
        const auto * it = generic_id_map.map.find(keys[i]);
        if (!it)
            throwUnknownID(id_data, i);
        res.push_back(groups[it->getMapped()]);
    }

    return res;
}


}
