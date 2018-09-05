#include <functional>
#include <sstream>
#include <memory>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Common/BitHelpers.h>
#include <Common/randomSeed.h>
#include <Common/HashTable/Hash.h>
#include <Common/Stopwatch.h>
#include <Common/ProfilingScopedRWLock.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/typeid_cast.h>
#include <Dictionaries/CacheDictionary.h>
#include <Dictionaries/DictionaryBlockInputStream.h>
#include <ext/size.h>
#include <ext/range.h>
#include <ext/map.h>


namespace ProfileEvents
{
    extern const Event DictCacheKeysRequested;
    extern const Event DictCacheKeysRequestedMiss;
    extern const Event DictCacheKeysRequestedFound;
    extern const Event DictCacheKeysExpired;
    extern const Event DictCacheKeysNotFound;
    extern const Event DictCacheKeysHit;
    extern const Event DictCacheRequestTimeNs;
    extern const Event DictCacheRequests;
    extern const Event DictCacheLockWriteNs;
    extern const Event DictCacheLockReadNs;
}

namespace CurrentMetrics
{
    extern const Metric DictCacheRequests;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
}


inline size_t CacheDictionary::getCellIdx(const Key id) const
{
    const auto hash = intHash64(id);
    const auto idx = hash & size_overlap_mask;
    return idx;
}


CacheDictionary::CacheDictionary(const std::string & name, const DictionaryStructure & dict_struct,
    DictionarySourcePtr source_ptr, const DictionaryLifetime dict_lifetime,
    const size_t size)
    : name{name}, dict_struct(dict_struct),
        source_ptr{std::move(source_ptr)}, dict_lifetime(dict_lifetime),
        size{roundUpToPowerOfTwoOrZero(std::max(size, size_t(max_collision_length)))},
        size_overlap_mask{this->size - 1},
        cells{this->size},
        rnd_engine(randomSeed())
{
    if (!this->source_ptr->supportsSelectiveLoad())
        throw Exception{name + ": source cannot be used with CacheDictionary", ErrorCodes::UNSUPPORTED_METHOD};

    createAttributes();
}

CacheDictionary::CacheDictionary(const CacheDictionary & other)
    : CacheDictionary{other.name, other.dict_struct, other.source_ptr->clone(), other.dict_lifetime, other.size}
{}


void CacheDictionary::toParent(const PaddedPODArray<Key> & ids, PaddedPODArray<Key> & out) const
{
    const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);

    getItemsNumber<UInt64>(*hierarchical_attribute, ids, out, [&] (const size_t) { return null_value; });
}


/// Allow to use single value in same way as array.
static inline CacheDictionary::Key getAt(const PaddedPODArray<CacheDictionary::Key> & arr, const size_t idx) { return arr[idx]; }
static inline CacheDictionary::Key getAt(const CacheDictionary::Key & value, const size_t) { return value; }


template <typename AncestorType>
void CacheDictionary::isInImpl(
    const PaddedPODArray<Key> & child_ids,
    const AncestorType & ancestor_ids,
    PaddedPODArray<UInt8> & out) const
{
    /// Transform all children to parents until ancestor id or null_value will be reached.

    size_t out_size = out.size();
    memset(out.data(), 0xFF, out_size);        /// 0xFF means "not calculated"

    const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);

    PaddedPODArray<Key> children(out_size);
    PaddedPODArray<Key> parents(child_ids.begin(), child_ids.end());

    while (true)
    {
        size_t out_idx = 0;
        size_t parents_idx = 0;
        size_t new_children_idx = 0;

        while (out_idx < out_size)
        {
            /// Already calculated
            if (out[out_idx] != 0xFF)
            {
                ++out_idx;
                continue;
            }

            /// No parent
            if (parents[parents_idx] == null_value)
            {
                out[out_idx] = 0;
            }
            /// Found ancestor
            else if (parents[parents_idx] == getAt(ancestor_ids, parents_idx))
            {
                out[out_idx] = 1;
            }
            /// Loop detected
            else if (children[new_children_idx] == parents[parents_idx])
            {
                out[out_idx] = 1;
            }
            /// Found intermediate parent, add this value to search at next loop iteration
            else
            {
                children[new_children_idx] = parents[parents_idx];
                ++new_children_idx;
            }

            ++out_idx;
            ++parents_idx;
        }

        if (new_children_idx == 0)
            break;

        /// Transform all children to its parents.
        children.resize(new_children_idx);
        parents.resize(new_children_idx);

        toParent(children, parents);
    }
}

void CacheDictionary::isInVectorVector(
    const PaddedPODArray<Key> & child_ids,
    const PaddedPODArray<Key> & ancestor_ids,
    PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_ids, ancestor_ids, out);
}

void CacheDictionary::isInVectorConstant(
    const PaddedPODArray<Key> & child_ids,
    const Key ancestor_id,
    PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_ids, ancestor_id, out);
}

void CacheDictionary::isInConstantVector(
    const Key child_id,
    const PaddedPODArray<Key> & ancestor_ids,
    PaddedPODArray<UInt8> & out) const
{
    /// Special case with single child value.

    const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);

    PaddedPODArray<Key> child(1, child_id);
    PaddedPODArray<Key> parent(1);
    std::vector<Key> ancestors(1, child_id);

    /// Iteratively find all ancestors for child.
    while (true)
    {
        toParent(child, parent);

        if (parent[0] == null_value)
            break;

        child[0] = parent[0];
        ancestors.push_back(parent[0]);
    }

    /// Assuming short hierarchy, so linear search is Ok.
    for (size_t i = 0, out_size = out.size(); i < out_size; ++i)
        out[i] = std::find(ancestors.begin(), ancestors.end(), ancestor_ids[i]) != ancestors.end();
}


#define DECLARE(TYPE)\
void CacheDictionary::get##TYPE(const std::string & attribute_name, const PaddedPODArray<Key> & ids, PaddedPODArray<TYPE> & out) const\
{\
    auto & attribute = getAttribute(attribute_name);\
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
        throw Exception{name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type), ErrorCodes::TYPE_MISMATCH};\
    \
    const auto null_value = std::get<TYPE>(attribute.null_values);\
    \
    getItemsNumber<TYPE>(attribute, ids, out, [&] (const size_t) { return null_value; });\
}
DECLARE(UInt8)
DECLARE(UInt16)
DECLARE(UInt32)
DECLARE(UInt64)
DECLARE(UInt128)
DECLARE(Int8)
DECLARE(Int16)
DECLARE(Int32)
DECLARE(Int64)
DECLARE(Float32)
DECLARE(Float64)
#undef DECLARE

void CacheDictionary::getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ColumnString * out) const
{
    auto & attribute = getAttribute(attribute_name);
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
        throw Exception{name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type), ErrorCodes::TYPE_MISMATCH};

    const auto null_value = StringRef{std::get<String>(attribute.null_values)};

    getItemsString(attribute, ids, out, [&] (const size_t) { return null_value; });
}

#define DECLARE(TYPE)\
void CacheDictionary::get##TYPE(\
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const PaddedPODArray<TYPE> & def,\
    PaddedPODArray<TYPE> & out) const\
{\
    auto & attribute = getAttribute(attribute_name);\
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
        throw Exception{name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type), ErrorCodes::TYPE_MISMATCH};\
    \
    getItemsNumber<TYPE>(attribute, ids, out, [&] (const size_t row) { return def[row]; });\
}
DECLARE(UInt8)
DECLARE(UInt16)
DECLARE(UInt32)
DECLARE(UInt64)
DECLARE(UInt128)
DECLARE(Int8)
DECLARE(Int16)
DECLARE(Int32)
DECLARE(Int64)
DECLARE(Float32)
DECLARE(Float64)
#undef DECLARE

void CacheDictionary::getString(
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def,
    ColumnString * const out) const
{
    auto & attribute = getAttribute(attribute_name);
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
        throw Exception{name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type), ErrorCodes::TYPE_MISMATCH};

    getItemsString(attribute, ids, out, [&] (const size_t row) { return def->getDataAt(row); });
}

#define DECLARE(TYPE)\
void CacheDictionary::get##TYPE(\
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const TYPE def, PaddedPODArray<TYPE> & out) const\
{\
    auto & attribute = getAttribute(attribute_name);\
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
        throw Exception{name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type), ErrorCodes::TYPE_MISMATCH};\
    \
    getItemsNumber<TYPE>(attribute, ids, out, [&] (const size_t) { return def; });\
}
DECLARE(UInt8)
DECLARE(UInt16)
DECLARE(UInt32)
DECLARE(UInt64)
DECLARE(UInt128)
DECLARE(Int8)
DECLARE(Int16)
DECLARE(Int32)
DECLARE(Int64)
DECLARE(Float32)
DECLARE(Float64)
#undef DECLARE

void CacheDictionary::getString(
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def,
    ColumnString * const out) const
{
    auto & attribute = getAttribute(attribute_name);
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
        throw Exception{name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type), ErrorCodes::TYPE_MISMATCH};

    getItemsString(attribute, ids, out, [&] (const size_t) { return StringRef{def}; });
}


/// returns cell_idx (always valid for replacing), 'cell is valid' flag, 'cell is outdated' flag
/// true  false   found and valid
/// false true    not found (something outdated, maybe our cell)
/// false false   not found (other id stored with valid data)
/// true  true    impossible
///
/// todo: split this func to two: find_for_get and find_for_set
CacheDictionary::FindResult CacheDictionary::findCellIdx(const Key & id, const CellMetadata::time_point_t now) const
{
    auto pos = getCellIdx(id);
    auto oldest_id = pos;
    auto oldest_time = CellMetadata::time_point_t::max();
    const auto stop = pos + max_collision_length;
    for (; pos < stop; ++pos)
    {
        const auto cell_idx = pos & size_overlap_mask;
        const auto & cell = cells[cell_idx];

        if (cell.id != id)
        {
            /// maybe we already found nearest expired cell (try minimize collision_length on insert)
            if (oldest_time > now && oldest_time > cell.expiresAt())
            {
                oldest_time = cell.expiresAt();
                oldest_id = cell_idx;
            }
            continue;
        }

        if (cell.expiresAt() < now)
        {
            return {cell_idx, false, true};
        }

        return {cell_idx, true, false};
    }

    return {oldest_id, false, false};
}

void CacheDictionary::has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const
{
    /// Mapping: <id> -> { all indices `i` of `ids` such that `ids[i]` = <id> }
    std::unordered_map<Key, std::vector<size_t>> outdated_ids;

    size_t cache_expired = 0, cache_not_found = 0, cache_hit = 0;

    const auto rows = ext::size(ids);
    {
        const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

        const auto now = std::chrono::system_clock::now();
        /// fetch up-to-date values, decide which ones require update
        for (const auto row : ext::range(0, rows))
        {
            const auto id = ids[row];
            const auto find_result = findCellIdx(id, now);
            const auto & cell_idx = find_result.cell_idx;
            if (!find_result.valid)
            {
                outdated_ids[id].push_back(row);
                if (find_result.outdated)
                    ++cache_expired;
                else
                    ++cache_not_found;
            }
            else
            {
                ++cache_hit;
                const auto & cell = cells[cell_idx];
                out[row] = !cell.isDefault();
            }
        }
    }

    ProfileEvents::increment(ProfileEvents::DictCacheKeysExpired, cache_expired);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysNotFound, cache_not_found);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysHit, cache_hit);

    query_count.fetch_add(rows, std::memory_order_relaxed);
    hit_count.fetch_add(rows - outdated_ids.size(), std::memory_order_release);

    if (outdated_ids.empty())
        return;

    std::vector<Key> required_ids(outdated_ids.size());
    std::transform(std::begin(outdated_ids), std::end(outdated_ids), std::begin(required_ids),
        [] (auto & pair) { return pair.first; });

    /// request new values
    update(required_ids,
    [&] (const auto id, const auto)
    {
        for (const auto row : outdated_ids[id])
            out[row] = true;
    },
    [&] (const auto id, const auto)
    {
        for (const auto row : outdated_ids[id])
            out[row] = false;
    });
}


void CacheDictionary::createAttributes()
{
    const auto attributes_size = dict_struct.attributes.size();
    attributes.reserve(attributes_size);

    bytes_allocated += size * sizeof(CellMetadata);
    bytes_allocated += attributes_size * sizeof(attributes.front());

    for (const auto & attribute : dict_struct.attributes)
    {
        attribute_index_by_name.emplace(attribute.name, attributes.size());
        attributes.push_back(createAttributeWithType(attribute.underlying_type, attribute.null_value));

        if (attribute.hierarchical)
        {
            hierarchical_attribute = &attributes.back();

            if (hierarchical_attribute->type != AttributeUnderlyingType::UInt64)
                throw Exception{name + ": hierarchical attribute must be UInt64.", ErrorCodes::TYPE_MISMATCH};
        }
    }
}

CacheDictionary::Attribute CacheDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    Attribute attr{type, {}, {}};

    switch (type)
    {
        case AttributeUnderlyingType::UInt8:
            std::get<UInt8>(attr.null_values) = null_value.get<UInt64>();
            std::get<ContainerPtrType<UInt8>>(attr.arrays) = std::make_unique<ContainerType<UInt8>>(size);
            bytes_allocated += size * sizeof(UInt8);
            break;
        case AttributeUnderlyingType::UInt16:
            std::get<UInt16>(attr.null_values) = null_value.get<UInt64>();
            std::get<ContainerPtrType<UInt16>>(attr.arrays) = std::make_unique<ContainerType<UInt16>>(size);
            bytes_allocated += size * sizeof(UInt16);
            break;
        case AttributeUnderlyingType::UInt32:
            std::get<UInt32>(attr.null_values) = null_value.get<UInt64>();
            std::get<ContainerPtrType<UInt32>>(attr.arrays) = std::make_unique<ContainerType<UInt32>>(size);
            bytes_allocated += size * sizeof(UInt32);
            break;
        case AttributeUnderlyingType::UInt64:
            std::get<UInt64>(attr.null_values) = null_value.get<UInt64>();
            std::get<ContainerPtrType<UInt64>>(attr.arrays) = std::make_unique<ContainerType<UInt64>>(size);
            bytes_allocated += size * sizeof(UInt64);
            break;
        case AttributeUnderlyingType::UInt128:
            std::get<UInt128>(attr.null_values) = null_value.get<UInt128>();
            std::get<ContainerPtrType<UInt128>>(attr.arrays) = std::make_unique<ContainerType<UInt128>>(size);
            bytes_allocated += size * sizeof(UInt128);
            break;
        case AttributeUnderlyingType::Int8:
            std::get<Int8>(attr.null_values) = null_value.get<Int64>();
            std::get<ContainerPtrType<Int8>>(attr.arrays) = std::make_unique<ContainerType<Int8>>(size);
            bytes_allocated += size * sizeof(Int8);
            break;
        case AttributeUnderlyingType::Int16:
            std::get<Int16>(attr.null_values) = null_value.get<Int64>();
            std::get<ContainerPtrType<Int16>>(attr.arrays) = std::make_unique<ContainerType<Int16>>(size);
            bytes_allocated += size * sizeof(Int16);
            break;
        case AttributeUnderlyingType::Int32:
            std::get<Int32>(attr.null_values) = null_value.get<Int64>();
            std::get<ContainerPtrType<Int32>>(attr.arrays) = std::make_unique<ContainerType<Int32>>(size);
            bytes_allocated += size * sizeof(Int32);
            break;
        case AttributeUnderlyingType::Int64:
            std::get<Int64>(attr.null_values) = null_value.get<Int64>();
            std::get<ContainerPtrType<Int64>>(attr.arrays) = std::make_unique<ContainerType<Int64>>(size);
            bytes_allocated += size * sizeof(Int64);
            break;
        case AttributeUnderlyingType::Float32:
            std::get<Float32>(attr.null_values) = null_value.get<Float64>();
            std::get<ContainerPtrType<Float32>>(attr.arrays) = std::make_unique<ContainerType<Float32>>(size);
            bytes_allocated += size * sizeof(Float32);
            break;
        case AttributeUnderlyingType::Float64:
            std::get<Float64>(attr.null_values) = null_value.get<Float64>();
            std::get<ContainerPtrType<Float64>>(attr.arrays) = std::make_unique<ContainerType<Float64>>(size);
            bytes_allocated += size * sizeof(Float64);
            break;
        case AttributeUnderlyingType::String:
            std::get<String>(attr.null_values) = null_value.get<String>();
            std::get<ContainerPtrType<StringRef>>(attr.arrays) = std::make_unique<ContainerType<StringRef>>(size);
            bytes_allocated += size * sizeof(StringRef);
            if (!string_arena)
                string_arena = std::make_unique<ArenaWithFreeLists>();
            break;
    }

    return attr;
}


template <typename OutputType, typename DefaultGetter>
void CacheDictionary::getItemsNumber(
    Attribute & attribute,
    const PaddedPODArray<Key> & ids,
    PaddedPODArray<OutputType> & out,
    DefaultGetter && get_default) const
{
    if (false) {}
#define DISPATCH(TYPE) \
    else if (attribute.type == AttributeUnderlyingType::TYPE) \
        getItemsNumberImpl<TYPE, OutputType>(attribute, ids, out, std::forward<DefaultGetter>(get_default));
    DISPATCH(UInt8)
    DISPATCH(UInt16)
    DISPATCH(UInt32)
    DISPATCH(UInt64)
    DISPATCH(UInt128)
    DISPATCH(Int8)
    DISPATCH(Int16)
    DISPATCH(Int32)
    DISPATCH(Int64)
    DISPATCH(Float32)
    DISPATCH(Float64)
#undef DISPATCH
    else
        throw Exception("Unexpected type of attribute: " + toString(attribute.type), ErrorCodes::LOGICAL_ERROR);
}

template <typename AttributeType, typename OutputType, typename DefaultGetter>
void CacheDictionary::getItemsNumberImpl(
    Attribute & attribute,
    const PaddedPODArray<Key> & ids,
    PaddedPODArray<OutputType> & out,
    DefaultGetter && get_default) const
{
    /// Mapping: <id> -> { all indices `i` of `ids` such that `ids[i]` = <id> }
    std::unordered_map<Key, std::vector<size_t>> outdated_ids;
    auto & attribute_array = std::get<ContainerPtrType<AttributeType>>(attribute.arrays);
    const auto rows = ext::size(ids);

    size_t cache_expired = 0, cache_not_found = 0, cache_hit = 0;

    {
        const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

        const auto now = std::chrono::system_clock::now();
        /// fetch up-to-date values, decide which ones require update
        for (const auto row : ext::range(0, rows))
        {
            const auto id = ids[row];

            /** cell should be updated if either:
                *    1. ids do not match,
                *    2. cell has expired,
                *    3. explicit defaults were specified and cell was set default. */

            const auto find_result = findCellIdx(id, now);
            if (!find_result.valid)
            {
                outdated_ids[id].push_back(row);
                if (find_result.outdated)
                    ++cache_expired;
                else
                    ++cache_not_found;
            }
            else
            {
                ++cache_hit;
                const auto & cell_idx = find_result.cell_idx;
                const auto & cell = cells[cell_idx];
                out[row] = cell.isDefault() ? get_default(row) : static_cast<OutputType>(attribute_array[cell_idx]);
            }
        }
    }

    ProfileEvents::increment(ProfileEvents::DictCacheKeysExpired, cache_expired);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysNotFound, cache_not_found);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysHit, cache_hit);

    query_count.fetch_add(rows, std::memory_order_relaxed);
    hit_count.fetch_add(rows - outdated_ids.size(), std::memory_order_release);

    if (outdated_ids.empty())
        return;

    std::vector<Key> required_ids(outdated_ids.size());
    std::transform(std::begin(outdated_ids), std::end(outdated_ids), std::begin(required_ids),
        [] (auto & pair) { return pair.first; });

    /// request new values
    update(required_ids,
    [&] (const auto id, const auto cell_idx)
    {
        const auto attribute_value = attribute_array[cell_idx];

        for (const size_t row : outdated_ids[id])
            out[row] = static_cast<OutputType>(attribute_value);
    },
    [&] (const auto id, const auto)
    {
        for (const size_t row : outdated_ids[id])
            out[row] = get_default(row);
    });
}

template <typename DefaultGetter>
void CacheDictionary::getItemsString(
    Attribute & attribute,
    const PaddedPODArray<Key> & ids,
    ColumnString * out,
    DefaultGetter && get_default) const
{
    const auto rows = ext::size(ids);

    /// save on some allocations
    out->getOffsets().reserve(rows);

    auto & attribute_array = std::get<ContainerPtrType<StringRef>>(attribute.arrays);

    auto found_outdated_values = false;

    /// perform optimistic version, fallback to pessimistic if failed
    {
        const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

        const auto now = std::chrono::system_clock::now();
        /// fetch up-to-date values, discard on fail
        for (const auto row : ext::range(0, rows))
        {
            const auto id = ids[row];

            const auto find_result = findCellIdx(id, now);
            if (!find_result.valid)
            {
                found_outdated_values = true;
                break;
            }
            else
            {
                const auto & cell_idx = find_result.cell_idx;
                const auto & cell = cells[cell_idx];
                const auto string_ref = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];
                out->insertData(string_ref.data, string_ref.size);
            }
        }
    }

    /// optimistic code completed successfully
    if (!found_outdated_values)
    {
        query_count.fetch_add(rows, std::memory_order_relaxed);
        hit_count.fetch_add(rows, std::memory_order_release);
        return;
    }

    /// now onto the pessimistic one, discard possible partial results from the optimistic path
    out->getChars().resize_assume_reserved(0);
    out->getOffsets().resize_assume_reserved(0);

    /// Mapping: <id> -> { all indices `i` of `ids` such that `ids[i]` = <id> }
    std::unordered_map<Key, std::vector<size_t>> outdated_ids;
    /// we are going to store every string separately
    std::unordered_map<Key, String> map;

    size_t total_length = 0;
    size_t cache_expired = 0, cache_not_found = 0, cache_hit = 0;
    {
        const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

        const auto now = std::chrono::system_clock::now();
        for (const auto row : ext::range(0, ids.size()))
        {
            const auto id = ids[row];

            const auto find_result = findCellIdx(id, now);
            if (!find_result.valid)
            {
                outdated_ids[id].push_back(row);
                if (find_result.outdated)
                    ++cache_expired;
                else
                    ++cache_not_found;
            }
            else
            {
                ++cache_hit;
                const auto & cell_idx = find_result.cell_idx;
                const auto & cell = cells[cell_idx];
                const auto string_ref = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];

                if (!cell.isDefault())
                    map[id] = String{string_ref};

                total_length += string_ref.size + 1;
            }
        }
    }

    ProfileEvents::increment(ProfileEvents::DictCacheKeysExpired, cache_expired);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysNotFound, cache_not_found);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysHit, cache_hit);

    query_count.fetch_add(rows, std::memory_order_relaxed);
    hit_count.fetch_add(rows - outdated_ids.size(), std::memory_order_release);

    /// request new values
    if (!outdated_ids.empty())
    {
        std::vector<Key> required_ids(outdated_ids.size());
        std::transform(std::begin(outdated_ids), std::end(outdated_ids), std::begin(required_ids),
            [] (auto & pair) { return pair.first; });

        update(required_ids,
        [&] (const auto id, const auto cell_idx)
        {
            const auto attribute_value = attribute_array[cell_idx];

            map[id] = String{attribute_value};
            total_length += (attribute_value.size + 1) * outdated_ids[id].size();
        },
        [&] (const auto id, const auto)
        {
            for (const auto row : outdated_ids[id])
                total_length += get_default(row).size + 1;
        });
    }

    out->getChars().reserve(total_length);

    for (const auto row : ext::range(0, ext::size(ids)))
    {
        const auto id = ids[row];
        const auto it = map.find(id);

        const auto string_ref = it != std::end(map) ? StringRef{it->second} : get_default(row);
        out->insertData(string_ref.data, string_ref.size);
    }
}

template <typename PresentIdHandler, typename AbsentIdHandler>
void CacheDictionary::update(
    const std::vector<Key> & requested_ids,
    PresentIdHandler && on_cell_updated,
    AbsentIdHandler && on_id_not_found) const
{
    std::unordered_map<Key, UInt8> remaining_ids{requested_ids.size()};
    for (const auto id : requested_ids)
        remaining_ids.insert({ id, 0 });

    std::uniform_int_distribution<UInt64> distribution
    {
        dict_lifetime.min_sec,
        dict_lifetime.max_sec
    };

    const ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};

    {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::DictCacheRequests};
        Stopwatch watch;
        auto stream = source_ptr->loadIds(requested_ids);
        stream->readPrefix();

        const auto now = std::chrono::system_clock::now();

        while (const auto block = stream->read())
        {
            const auto id_column = typeid_cast<const ColumnUInt64 *>(block.safeGetByPosition(0).column.get());
            if (!id_column)
                throw Exception{name + ": id column has type different from UInt64.", ErrorCodes::TYPE_MISMATCH};

            const auto & ids = id_column->getData();

            /// cache column pointers
            const auto column_ptrs = ext::map<std::vector>(ext::range(0, attributes.size()), [&block] (size_t i)
            {
                return block.safeGetByPosition(i + 1).column.get();
            });

            for (const auto i : ext::range(0, ids.size()))
            {
                const auto id = ids[i];

                const auto find_result = findCellIdx(id, now);
                const auto & cell_idx = find_result.cell_idx;

                auto & cell = cells[cell_idx];

                for (const auto attribute_idx : ext::range(0, attributes.size()))
                {
                    const auto & attribute_column = *column_ptrs[attribute_idx];
                    auto & attribute = attributes[attribute_idx];

                    setAttributeValue(attribute, cell_idx, attribute_column[i]);
                }

                /// if cell id is zero and zero does not map to this cell, then the cell is unused
                if (cell.id == 0 && cell_idx != zero_cell_idx)
                    element_count.fetch_add(1, std::memory_order_relaxed);

                cell.id = id;
                if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
                    cell.setExpiresAt(std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)});
                else
                    cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());

                /// inform caller
                on_cell_updated(id, cell_idx);
                /// mark corresponding id as found
                remaining_ids[id] = 1;
            }
        }

        stream->readSuffix();

        ProfileEvents::increment(ProfileEvents::DictCacheKeysRequested, requested_ids.size());
        ProfileEvents::increment(ProfileEvents::DictCacheRequestTimeNs, watch.elapsed());
    }

    size_t not_found_num = 0, found_num = 0;

    const auto now = std::chrono::system_clock::now();
    /// Check which ids have not been found and require setting null_value
    for (const auto & id_found_pair : remaining_ids)
    {
        if (id_found_pair.second)
        {
            ++found_num;
            continue;
        }
        ++not_found_num;

        const auto id = id_found_pair.first;

        const auto find_result = findCellIdx(id, now);
        const auto & cell_idx = find_result.cell_idx;

        auto & cell = cells[cell_idx];

        /// Set null_value for each attribute
        for (auto & attribute : attributes)
            setDefaultAttributeValue(attribute, cell_idx);

        /// Check if cell had not been occupied before and increment element counter if it hadn't
        if (cell.id == 0 && cell_idx != zero_cell_idx)
            element_count.fetch_add(1, std::memory_order_relaxed);

        cell.id = id;
        if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
            cell.setExpiresAt(std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)});
        else
            cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());

        cell.setDefault();

        /// inform caller that the cell has not been found
        on_id_not_found(id, cell_idx);
    }

    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedMiss, not_found_num);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedFound, found_num);
    ProfileEvents::increment(ProfileEvents::DictCacheRequests);
}


void CacheDictionary::setDefaultAttributeValue(Attribute & attribute, const Key idx) const
{
    switch (attribute.type)
    {
        case AttributeUnderlyingType::UInt8: std::get<ContainerPtrType<UInt8>>(attribute.arrays)[idx] = std::get<UInt8>(attribute.null_values); break;
        case AttributeUnderlyingType::UInt16: std::get<ContainerPtrType<UInt16>>(attribute.arrays)[idx] = std::get<UInt16>(attribute.null_values); break;
        case AttributeUnderlyingType::UInt32: std::get<ContainerPtrType<UInt32>>(attribute.arrays)[idx] = std::get<UInt32>(attribute.null_values); break;
        case AttributeUnderlyingType::UInt64: std::get<ContainerPtrType<UInt64>>(attribute.arrays)[idx] = std::get<UInt64>(attribute.null_values); break;
        case AttributeUnderlyingType::UInt128: std::get<ContainerPtrType<UInt128>>(attribute.arrays)[idx] = std::get<UInt128>(attribute.null_values); break;
        case AttributeUnderlyingType::Int8: std::get<ContainerPtrType<Int8>>(attribute.arrays)[idx] = std::get<Int8>(attribute.null_values); break;
        case AttributeUnderlyingType::Int16: std::get<ContainerPtrType<Int16>>(attribute.arrays)[idx] = std::get<Int16>(attribute.null_values); break;
        case AttributeUnderlyingType::Int32: std::get<ContainerPtrType<Int32>>(attribute.arrays)[idx] = std::get<Int32>(attribute.null_values); break;
        case AttributeUnderlyingType::Int64: std::get<ContainerPtrType<Int64>>(attribute.arrays)[idx] = std::get<Int64>(attribute.null_values); break;
        case AttributeUnderlyingType::Float32: std::get<ContainerPtrType<Float32>>(attribute.arrays)[idx] = std::get<Float32>(attribute.null_values); break;
        case AttributeUnderlyingType::Float64: std::get<ContainerPtrType<Float64>>(attribute.arrays)[idx] = std::get<Float64>(attribute.null_values); break;
        case AttributeUnderlyingType::String:
        {
            const auto & null_value_ref = std::get<String>(attribute.null_values);
            auto & string_ref = std::get<ContainerPtrType<StringRef>>(attribute.arrays)[idx];

            if (string_ref.data != null_value_ref.data())
            {
                if (string_ref.data)
                    string_arena->free(const_cast<char *>(string_ref.data), string_ref.size);

                string_ref = StringRef{null_value_ref};
            }

            break;
        }
    }
}

void CacheDictionary::setAttributeValue(Attribute & attribute, const Key idx, const Field & value) const
{
    switch (attribute.type)
    {
        case AttributeUnderlyingType::UInt8: std::get<ContainerPtrType<UInt8>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
        case AttributeUnderlyingType::UInt16: std::get<ContainerPtrType<UInt16>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
        case AttributeUnderlyingType::UInt32: std::get<ContainerPtrType<UInt32>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
        case AttributeUnderlyingType::UInt64: std::get<ContainerPtrType<UInt64>>(attribute.arrays)[idx] = value.get<UInt64>(); break;
        case AttributeUnderlyingType::UInt128: std::get<ContainerPtrType<UInt128>>(attribute.arrays)[idx] = value.get<UInt128>(); break;
        case AttributeUnderlyingType::Int8: std::get<ContainerPtrType<Int8>>(attribute.arrays)[idx] = value.get<Int64>(); break;
        case AttributeUnderlyingType::Int16: std::get<ContainerPtrType<Int16>>(attribute.arrays)[idx] = value.get<Int64>(); break;
        case AttributeUnderlyingType::Int32: std::get<ContainerPtrType<Int32>>(attribute.arrays)[idx] = value.get<Int64>(); break;
        case AttributeUnderlyingType::Int64: std::get<ContainerPtrType<Int64>>(attribute.arrays)[idx] = value.get<Int64>(); break;
        case AttributeUnderlyingType::Float32: std::get<ContainerPtrType<Float32>>(attribute.arrays)[idx] = value.get<Float64>(); break;
        case AttributeUnderlyingType::Float64: std::get<ContainerPtrType<Float64>>(attribute.arrays)[idx] = value.get<Float64>(); break;
        case AttributeUnderlyingType::String:
        {
            const auto & string = value.get<String>();
            auto & string_ref = std::get<ContainerPtrType<StringRef>>(attribute.arrays)[idx];
            const auto & null_value_ref = std::get<String>(attribute.null_values);

            /// free memory unless it points to a null_value
            if (string_ref.data && string_ref.data != null_value_ref.data())
                string_arena->free(const_cast<char *>(string_ref.data), string_ref.size);

            const auto str_size = string.size();
            if (str_size != 0)
            {
                auto string_ptr = string_arena->alloc(str_size + 1);
                std::copy(string.data(), string.data() + str_size + 1, string_ptr);
                string_ref = StringRef{string_ptr, str_size};
            }
            else
                string_ref = {};

            break;
        }
    }
}

CacheDictionary::Attribute & CacheDictionary::getAttribute(const std::string & attribute_name) const
{
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == std::end(attribute_index_by_name))
        throw Exception{name + ": no such attribute '" + attribute_name + "'", ErrorCodes::BAD_ARGUMENTS};

    return attributes[it->second];
}

bool CacheDictionary::isEmptyCell(const UInt64 idx) const
{
    return (idx != zero_cell_idx && cells[idx].id == 0) || (cells[idx].data
        == ext::safe_bit_cast<CellMetadata::time_point_urep_t>(CellMetadata::time_point_t()));
}

PaddedPODArray<CacheDictionary::Key> CacheDictionary::getCachedIds() const
{
    const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

    PaddedPODArray<Key> array;
    for (size_t idx = 0; idx < cells.size(); ++idx)
    {
        auto & cell = cells[idx];
        if (!isEmptyCell(idx) && !cells[idx].isDefault())
        {
            array.push_back(cell.id);
        }
    }
    return array;
}

BlockInputStreamPtr CacheDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    using BlockInputStreamType = DictionaryBlockInputStream<CacheDictionary, Key>;
    return std::make_shared<BlockInputStreamType>(shared_from_this(), max_block_size, getCachedIds(), column_names);
}


}
