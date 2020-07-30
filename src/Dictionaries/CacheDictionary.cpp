#include "CacheDictionary.h"

#include <memory>
#include <Columns/ColumnString.h>
#include <Common/BitHelpers.h>
#include <Common/CurrentMetrics.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <Common/ProfilingScopedRWLock.h>
#include <Common/randomSeed.h>
#include <Common/typeid_cast.h>
#include <Core/Defines.h>
#include <ext/range.h>
#include <ext/size.h>
#include <Common/setThreadName.h>
#include "CacheDictionary.inc.h"
#include "DictionaryBlockInputStream.h"
#include "DictionaryFactory.h"


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
    extern const int CACHE_DICTIONARY_UPDATE_FAIL;
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
    extern const int TOO_SMALL_BUFFER_SIZE;
    extern const int TIMEOUT_EXCEEDED;
}


inline size_t CacheDictionary::getCellIdx(const Key id) const
{
    const auto hash = intHash64(id);
    const auto idx = hash & size_overlap_mask;
    return idx;
}


CacheDictionary::CacheDictionary(
    const std::string & database_,
    const std::string & name_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    DictionaryLifetime dict_lifetime_,
    size_t strict_max_lifetime_seconds_,
    size_t size_,
    bool allow_read_expired_keys_,
    size_t max_update_queue_size_,
    size_t update_queue_push_timeout_milliseconds_,
    size_t query_wait_timeout_milliseconds_,
    size_t max_threads_for_updates_)
    : database(database_)
    , name(name_)
    , full_name{database_.empty() ? name_ : (database_ + "." + name_)}
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , dict_lifetime(dict_lifetime_)
    , strict_max_lifetime_seconds(strict_max_lifetime_seconds_)
    , allow_read_expired_keys(allow_read_expired_keys_)
    , max_update_queue_size(max_update_queue_size_)
    , update_queue_push_timeout_milliseconds(update_queue_push_timeout_milliseconds_)
    , query_wait_timeout_milliseconds(query_wait_timeout_milliseconds_)
    , max_threads_for_updates(max_threads_for_updates_)
    , log(&Poco::Logger::get("ExternalDictionaries"))
    , size{roundUpToPowerOfTwoOrZero(std::max(size_, size_t(max_collision_length)))}
    , size_overlap_mask{this->size - 1}
    , cells{this->size}
    , rnd_engine(randomSeed())
    , update_queue(max_update_queue_size_)
    , update_pool(max_threads_for_updates)
{
    if (!source_ptr->supportsSelectiveLoad())
        throw Exception{full_name + ": source cannot be used with CacheDictionary", ErrorCodes::UNSUPPORTED_METHOD};

    createAttributes();
    for (size_t i = 0; i < max_threads_for_updates; ++i)
        update_pool.scheduleOrThrowOnError([this] { updateThreadFunction(); });
}

CacheDictionary::~CacheDictionary()
{
    finished = true;
    update_queue.clear();
    for (size_t i = 0; i < max_threads_for_updates; ++i)
    {
        auto empty_finishing_ptr = std::make_shared<UpdateUnit>(std::vector<Key>());
        update_queue.push(empty_finishing_ptr);
    }
    update_pool.wait();
}


void CacheDictionary::toParent(const PaddedPODArray<Key> & ids, PaddedPODArray<Key> & out) const
{
    const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);

    getItemsNumberImpl<UInt64, UInt64>(*hierarchical_attribute, ids, out, [&](const size_t) { return null_value; });
}


/// Allow to use single value in same way as array.
static inline CacheDictionary::Key getAt(const PaddedPODArray<CacheDictionary::Key> & arr, const size_t idx)
{
    return arr[idx];
}
static inline CacheDictionary::Key getAt(const CacheDictionary::Key & value, const size_t)
{
    return value;
}


template <typename AncestorType>
void CacheDictionary::isInImpl(const PaddedPODArray<Key> & child_ids, const AncestorType & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    /// Transform all children to parents until ancestor id or null_value will be reached.

    size_t out_size = out.size();
    memset(out.data(), 0xFF, out_size); /// 0xFF means "not calculated"

    const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);

    PaddedPODArray<Key> children(out_size, 0);
    PaddedPODArray<Key> parents(child_ids.begin(), child_ids.end());

    for (size_t i = 0; i < DBMS_HIERARCHICAL_DICTIONARY_MAX_DEPTH; ++i)
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
    const PaddedPODArray<Key> & child_ids, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_ids, ancestor_ids, out);
}

void CacheDictionary::isInVectorConstant(const PaddedPODArray<Key> & child_ids, const Key ancestor_id, PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_ids, ancestor_id, out);
}

void CacheDictionary::isInConstantVector(const Key child_id, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    /// Special case with single child value.

    const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);

    PaddedPODArray<Key> child(1, child_id);
    PaddedPODArray<Key> parent(1);
    std::vector<Key> ancestors(1, child_id);

    /// Iteratively find all ancestors for child.
    for (size_t i = 0; i < DBMS_HIERARCHICAL_DICTIONARY_MAX_DEPTH; ++i)
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

void CacheDictionary::getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ColumnString * out) const
{
    auto & attribute = getAttribute(attribute_name);
    checkAttributeType(full_name, attribute_name, attribute.type, AttributeUnderlyingType::utString);

    const auto null_value = StringRef{std::get<String>(attribute.null_values)};

    getItemsString(attribute, ids, out, [&](const size_t) { return null_value; });
}

void CacheDictionary::getString(
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def, ColumnString * const out) const
{
    auto & attribute = getAttribute(attribute_name);
    checkAttributeType(full_name, attribute_name, attribute.type, AttributeUnderlyingType::utString);

    getItemsString(attribute, ids, out, [&](const size_t row) { return def->getDataAt(row); });
}

void CacheDictionary::getString(
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def, ColumnString * const out) const
{
    auto & attribute = getAttribute(attribute_name);
    checkAttributeType(full_name, attribute_name, attribute.type, AttributeUnderlyingType::utString);

    getItemsString(attribute, ids, out, [&](const size_t) { return StringRef{def}; });
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
    /// There are three types of ids.
    /// - Valid ids. These ids are presented in local cache and their lifetime is not expired.
    /// - CacheExpired ids. Ids that are in local cache, but their values are rotted (lifetime is expired).
    /// - CacheNotFound ids. We have to go to external storage to know its value.

    /// Mapping: <id> -> { all indices `i` of `ids` such that `ids[i]` = <id> }
    std::unordered_map<Key, std::vector<size_t>> cache_expired_ids;
    std::unordered_map<Key, std::vector<size_t>> cache_not_found_ids;

    size_t cache_hit = 0;

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

            auto insert_to_answer_routine = [&] ()
            {
                out[row] = !cells[cell_idx].isDefault();
            };

            if (!find_result.valid)
            {
                if (find_result.outdated)
                {
                    /// Protection of reading very expired keys.
                    if (now > cells[find_result.cell_idx].strict_max)
                    {
                        cache_not_found_ids[id].push_back(row);
                        continue;
                    }

                    cache_expired_ids[id].push_back(row);

                    if (allow_read_expired_keys)
                        insert_to_answer_routine();
                }
                else
                {
                    cache_not_found_ids[id].push_back(row);
                }
            }
            else
            {
                ++cache_hit;
                insert_to_answer_routine();
            }
        }
    }

    ProfileEvents::increment(ProfileEvents::DictCacheKeysExpired, cache_expired_ids.size());
    ProfileEvents::increment(ProfileEvents::DictCacheKeysNotFound, cache_not_found_ids.size());
    ProfileEvents::increment(ProfileEvents::DictCacheKeysHit, cache_hit);

    query_count.fetch_add(rows, std::memory_order_relaxed);
    hit_count.fetch_add(rows - cache_expired_ids.size() - cache_not_found_ids.size(), std::memory_order_release);

    if (cache_not_found_ids.empty())
    {
        /// Nothing to update - return;
        if (cache_expired_ids.empty())
            return;

        if (allow_read_expired_keys)
        {
            std::vector<Key> required_expired_ids;
            required_expired_ids.reserve(cache_expired_ids.size());
            std::transform(
                    std::begin(cache_expired_ids), std::end(cache_expired_ids),
                    std::back_inserter(required_expired_ids), [](auto & pair) { return pair.first; });

            /// Callbacks are empty because we don't want to receive them after an unknown period of time.
            auto update_unit_ptr = std::make_shared<UpdateUnit>(required_expired_ids);

            tryPushToUpdateQueueOrThrow(update_unit_ptr);
            /// Update is async - no need to wait.
            return;
        }
    }

    /// At this point we have two situations.
    /// There may be both types of keys: cache_expired_ids and cache_not_found_ids.
    /// We will update them all synchronously.

    std::vector<Key> required_ids;
    required_ids.reserve(cache_not_found_ids.size() + cache_expired_ids.size());
    std::transform(
            std::begin(cache_not_found_ids), std::end(cache_not_found_ids),
            std::back_inserter(required_ids), [](auto & pair) { return pair.first; });
    std::transform(
            std::begin(cache_expired_ids), std::end(cache_expired_ids),
            std::back_inserter(required_ids), [](auto & pair) { return pair.first; });

    auto on_cell_updated = [&] (const Key id, const size_t)
    {
        for (const auto row : cache_not_found_ids[id])
            out[row] = true;
        for (const auto row : cache_expired_ids[id])
            out[row] = true;
    };

    auto on_id_not_found = [&] (const Key id, const size_t)
    {
        for (const auto row : cache_not_found_ids[id])
            out[row] = false;
        for (const auto row : cache_expired_ids[id])
            out[row] = true;
    };

    auto update_unit_ptr = std::make_shared<UpdateUnit>(required_ids, on_cell_updated, on_id_not_found);

    tryPushToUpdateQueueOrThrow(update_unit_ptr);
    waitForCurrentUpdateFinish(update_unit_ptr);
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

            if (hierarchical_attribute->type != AttributeUnderlyingType::utUInt64)
                throw Exception{full_name + ": hierarchical attribute must be UInt64.", ErrorCodes::TYPE_MISMATCH};
        }
    }
}

CacheDictionary::Attribute CacheDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    Attribute attr{type, {}, {}};

    switch (type)
    {
#define DISPATCH(TYPE) \
    case AttributeUnderlyingType::ut##TYPE: \
        attr.null_values = TYPE(null_value.get<NearestFieldType<TYPE>>()); /* NOLINT */ \
        attr.arrays = std::make_unique<ContainerType<TYPE>>(size); /* NOLINT */ \
        bytes_allocated += size * sizeof(TYPE); \
        break;
        DISPATCH(UInt8)
        DISPATCH(UInt16)
        DISPATCH(UInt32)
        DISPATCH(UInt64)
        DISPATCH(UInt128)
        DISPATCH(Int8)
        DISPATCH(Int16)
        DISPATCH(Int32)
        DISPATCH(Int64)
        DISPATCH(Decimal32)
        DISPATCH(Decimal64)
        DISPATCH(Decimal128)
        DISPATCH(Float32)
        DISPATCH(Float64)
#undef DISPATCH
        case AttributeUnderlyingType::utString:
            attr.null_values = null_value.get<String>();
            attr.arrays = std::make_unique<ContainerType<StringRef>>(size);
            bytes_allocated += size * sizeof(StringRef);
            if (!string_arena)
                string_arena = std::make_unique<ArenaWithFreeLists>();
            break;
    }

    return attr;
}

void CacheDictionary::setDefaultAttributeValue(Attribute & attribute, const Key idx) const
{
    switch (attribute.type)
    {
        case AttributeUnderlyingType::utUInt8:
            std::get<ContainerPtrType<UInt8>>(attribute.arrays)[idx] = std::get<UInt8>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utUInt16:
            std::get<ContainerPtrType<UInt16>>(attribute.arrays)[idx] = std::get<UInt16>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utUInt32:
            std::get<ContainerPtrType<UInt32>>(attribute.arrays)[idx] = std::get<UInt32>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utUInt64:
            std::get<ContainerPtrType<UInt64>>(attribute.arrays)[idx] = std::get<UInt64>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utUInt128:
            std::get<ContainerPtrType<UInt128>>(attribute.arrays)[idx] = std::get<UInt128>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utInt8:
            std::get<ContainerPtrType<Int8>>(attribute.arrays)[idx] = std::get<Int8>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utInt16:
            std::get<ContainerPtrType<Int16>>(attribute.arrays)[idx] = std::get<Int16>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utInt32:
            std::get<ContainerPtrType<Int32>>(attribute.arrays)[idx] = std::get<Int32>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utInt64:
            std::get<ContainerPtrType<Int64>>(attribute.arrays)[idx] = std::get<Int64>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utFloat32:
            std::get<ContainerPtrType<Float32>>(attribute.arrays)[idx] = std::get<Float32>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utFloat64:
            std::get<ContainerPtrType<Float64>>(attribute.arrays)[idx] = std::get<Float64>(attribute.null_values);
            break;

        case AttributeUnderlyingType::utDecimal32:
            std::get<ContainerPtrType<Decimal32>>(attribute.arrays)[idx] = std::get<Decimal32>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utDecimal64:
            std::get<ContainerPtrType<Decimal64>>(attribute.arrays)[idx] = std::get<Decimal64>(attribute.null_values);
            break;
        case AttributeUnderlyingType::utDecimal128:
            std::get<ContainerPtrType<Decimal128>>(attribute.arrays)[idx] = std::get<Decimal128>(attribute.null_values);
            break;

        case AttributeUnderlyingType::utString:
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
        case AttributeUnderlyingType::utUInt8:
            std::get<ContainerPtrType<UInt8>>(attribute.arrays)[idx] = value.get<UInt64>();
            break;
        case AttributeUnderlyingType::utUInt16:
            std::get<ContainerPtrType<UInt16>>(attribute.arrays)[idx] = value.get<UInt64>();
            break;
        case AttributeUnderlyingType::utUInt32:
            std::get<ContainerPtrType<UInt32>>(attribute.arrays)[idx] = value.get<UInt64>();
            break;
        case AttributeUnderlyingType::utUInt64:
            std::get<ContainerPtrType<UInt64>>(attribute.arrays)[idx] = value.get<UInt64>();
            break;
        case AttributeUnderlyingType::utUInt128:
            std::get<ContainerPtrType<UInt128>>(attribute.arrays)[idx] = value.get<UInt128>();
            break;
        case AttributeUnderlyingType::utInt8:
            std::get<ContainerPtrType<Int8>>(attribute.arrays)[idx] = value.get<Int64>();
            break;
        case AttributeUnderlyingType::utInt16:
            std::get<ContainerPtrType<Int16>>(attribute.arrays)[idx] = value.get<Int64>();
            break;
        case AttributeUnderlyingType::utInt32:
            std::get<ContainerPtrType<Int32>>(attribute.arrays)[idx] = value.get<Int64>();
            break;
        case AttributeUnderlyingType::utInt64:
            std::get<ContainerPtrType<Int64>>(attribute.arrays)[idx] = value.get<Int64>();
            break;
        case AttributeUnderlyingType::utFloat32:
            std::get<ContainerPtrType<Float32>>(attribute.arrays)[idx] = value.get<Float64>();
            break;
        case AttributeUnderlyingType::utFloat64:
            std::get<ContainerPtrType<Float64>>(attribute.arrays)[idx] = value.get<Float64>();
            break;

        case AttributeUnderlyingType::utDecimal32:
            std::get<ContainerPtrType<Decimal32>>(attribute.arrays)[idx] = value.get<Decimal32>();
            break;
        case AttributeUnderlyingType::utDecimal64:
            std::get<ContainerPtrType<Decimal64>>(attribute.arrays)[idx] = value.get<Decimal64>();
            break;
        case AttributeUnderlyingType::utDecimal128:
            std::get<ContainerPtrType<Decimal128>>(attribute.arrays)[idx] = value.get<Decimal128>();
            break;

        case AttributeUnderlyingType::utString:
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
                auto * string_ptr = string_arena->alloc(str_size + 1);
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
        throw Exception{full_name + ": no such attribute '" + attribute_name + "'", ErrorCodes::BAD_ARGUMENTS};

    return attributes[it->second];
}

bool CacheDictionary::isEmptyCell(const UInt64 idx) const
{
    return (idx != zero_cell_idx && cells[idx].id == 0)
        || (cells[idx].data == ext::safe_bit_cast<CellMetadata::time_point_urep_t>(CellMetadata::time_point_t()));
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

std::exception_ptr CacheDictionary::getLastException() const
{
    const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};
    return last_exception;
}

void registerDictionaryCache(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        if (dict_struct.key)
            throw Exception{"'key' is not supported for dictionary of layout 'cache'",
                            ErrorCodes::UNSUPPORTED_METHOD};

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception{full_name
                                + ": elements .structure.range_min and .structure.range_max should be defined only "
                                  "for a dictionary of layout 'range_hashed'",
                            ErrorCodes::BAD_ARGUMENTS};
        const auto & layout_prefix = config_prefix + ".layout";

        const size_t size = config.getUInt64(layout_prefix + ".cache.size_in_cells");
        if (size == 0)
            throw Exception{full_name + ": dictionary of layout 'cache' cannot have 0 cells",
                            ErrorCodes::TOO_SMALL_BUFFER_SIZE};

        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);
        if (require_nonempty)
            throw Exception{full_name + ": dictionary of layout 'cache' cannot have 'require_nonempty' attribute set",
                            ErrorCodes::BAD_ARGUMENTS};

        const String database = config.getString(config_prefix + ".database", "");
        const String name = config.getString(config_prefix + ".name");
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

        const size_t strict_max_lifetime_seconds =
                config.getUInt64(layout_prefix + ".cache.strict_max_lifetime_seconds", static_cast<size_t>(dict_lifetime.max_sec));

        const size_t max_update_queue_size =
                config.getUInt64(layout_prefix + ".cache.max_update_queue_size", 100000);
        if (max_update_queue_size == 0)
            throw Exception{name + ": dictionary of layout 'cache' cannot have empty update queue of size 0",
                            ErrorCodes::TOO_SMALL_BUFFER_SIZE};

        const bool allow_read_expired_keys =
                config.getBool(layout_prefix + ".cache.allow_read_expired_keys", false);

        const size_t update_queue_push_timeout_milliseconds =
                config.getUInt64(layout_prefix + ".cache.update_queue_push_timeout_milliseconds", 10);
        if (update_queue_push_timeout_milliseconds < 10)
            throw Exception{name + ": dictionary of layout 'cache' have too little update_queue_push_timeout",
                            ErrorCodes::BAD_ARGUMENTS};

        const size_t query_wait_timeout_milliseconds =
                config.getUInt64(layout_prefix + ".cache.query_wait_timeout_milliseconds", 60000);

        const size_t max_threads_for_updates =
                config.getUInt64(layout_prefix + ".max_threads_for_updates", 4);
        if (max_threads_for_updates == 0)
            throw Exception{name + ": dictionary of layout 'cache' cannot have zero threads for updates.",
                            ErrorCodes::BAD_ARGUMENTS};

        return std::make_unique<CacheDictionary>(
                database,
                name,
                dict_struct,
                std::move(source_ptr),
                dict_lifetime,
                strict_max_lifetime_seconds,
                size,
                allow_read_expired_keys,
                max_update_queue_size,
                update_queue_push_timeout_milliseconds,
                query_wait_timeout_milliseconds,
                max_threads_for_updates);
    };
    factory.registerLayout("cache", create_layout, false);
}

void CacheDictionary::updateThreadFunction()
{
    setThreadName("AsyncUpdater");
    while (!finished)
    {
        UpdateUnitPtr first_popped;
        update_queue.pop(first_popped);

        if (finished)
            break;

        /// Here we pop as many unit pointers from update queue as we can.
        /// We fix current size to avoid livelock (or too long waiting),
        /// when this thread pops from the queue and other threads push to the queue.
        const size_t current_queue_size = update_queue.size();

        if (current_queue_size > 0)
            LOG_TRACE(log, "Performing bunch of keys update in cache dictionary with {} keys", current_queue_size + 1);

        std::vector<UpdateUnitPtr> update_request;
        update_request.reserve(current_queue_size + 1);
        update_request.emplace_back(first_popped);

        UpdateUnitPtr current_unit_ptr;

        while (update_request.size() < current_queue_size + 1 && update_queue.tryPop(current_unit_ptr))
            update_request.emplace_back(std::move(current_unit_ptr));

        BunchUpdateUnit bunch_update_unit(update_request);

        try
        {
            /// Update a bunch of ids.
            update(bunch_update_unit);

            /// Notify all threads about finished updating the bunch of ids
            /// where their own ids were included.
            std::unique_lock<std::mutex> lock(update_mutex);

            for (auto & unit_ptr: update_request)
                unit_ptr->is_done = true;

            is_update_finished.notify_all();
        }
        catch (...)
        {
            std::unique_lock<std::mutex> lock(update_mutex);
            /// It is a big trouble, because one bad query can make other threads fail with not relative exception.
            /// So at this point all threads (and queries) will receive the same exception.
            for (auto & unit_ptr: update_request)
                unit_ptr->current_exception = std::current_exception();

            is_update_finished.notify_all();
        }
    }
}

void CacheDictionary::waitForCurrentUpdateFinish(UpdateUnitPtr & update_unit_ptr) const
{
    std::unique_lock<std::mutex> update_lock(update_mutex);

    size_t timeout_for_wait = 100000;
    bool result = is_update_finished.wait_for(
            update_lock,
            std::chrono::milliseconds(timeout_for_wait),
            [&] { return update_unit_ptr->is_done || update_unit_ptr->current_exception; });

    if (!result)
    {
        std::lock_guard<std::mutex> callback_lock(update_unit_ptr->callback_mutex);
        /*
         * We acquire a lock here and store false to special variable to avoid SEGFAULT's.
         * Consider timeout for wait had expired and main query's thread ended with exception
         * or some other error. But the UpdateUnit with callbacks is left in the queue.
         * It has these callback that capture god knows what from the current thread
         * (most of the variables lies on the stack of finished thread) that
         * intended to do a synchronous update. AsyncUpdate thread can touch deallocated memory and explode.
         * */
        update_unit_ptr->can_use_callback = false;
        throw DB::Exception(
            "Dictionary " + getName() + " source seems unavailable, because " +
                toString(timeout_for_wait) + " timeout exceeded.", ErrorCodes::TIMEOUT_EXCEEDED);
    }


    if (update_unit_ptr->current_exception)
        std::rethrow_exception(update_unit_ptr->current_exception);
}

void CacheDictionary::tryPushToUpdateQueueOrThrow(UpdateUnitPtr & update_unit_ptr) const
{
    if (!update_queue.tryPush(update_unit_ptr, update_queue_push_timeout_milliseconds))
        throw DB::Exception(
                "Cannot push to internal update queue in dictionary " + getFullName() + ". Timelimit of " +
                std::to_string(update_queue_push_timeout_milliseconds) + " ms. exceeded. Current queue size is " +
                std::to_string(update_queue.size()), ErrorCodes::CACHE_DICTIONARY_UPDATE_FAIL);
}

void CacheDictionary::update(BunchUpdateUnit & bunch_update_unit) const
{
    CurrentMetrics::Increment metric_increment{CurrentMetrics::DictCacheRequests};
    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequested, bunch_update_unit.getRequestedIds().size());

    std::unordered_map<Key, UInt8> remaining_ids{bunch_update_unit.getRequestedIds().size()};
    for (const auto id : bunch_update_unit.getRequestedIds())
        remaining_ids.insert({id, 0});

    const auto now = std::chrono::system_clock::now();


    if (now > backoff_end_time.load())
    {
        try
        {
            auto current_source_ptr = getSourceAndUpdateIfNeeded();

            Stopwatch watch;

            BlockInputStreamPtr stream = current_source_ptr->loadIds(bunch_update_unit.getRequestedIds());
            stream->readPrefix();


            while (true)
            {
                Block block = stream->read();
                if (!block)
                    break;

                const auto * id_column = typeid_cast<const ColumnUInt64 *>(block.safeGetByPosition(0).column.get());
                if (!id_column)
                    throw Exception{name + ": id column has type different from UInt64.", ErrorCodes::TYPE_MISMATCH};

                const auto & ids = id_column->getData();

                /// cache column pointers
                const auto column_ptrs = ext::map<std::vector>(
                        ext::range(0, attributes.size()), [&block](size_t i) { return block.safeGetByPosition(i + 1).column.get(); });

                for (const auto i : ext::range(0, ids.size()))
                {
                    /// Modifying cache with write lock
                    ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};
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
                    {
                        std::uniform_int_distribution<UInt64> distribution{dict_lifetime.min_sec, dict_lifetime.max_sec};
                        cell.setExpiresAt(now + std::chrono::seconds{distribution(rnd_engine)});
                    }
                    else
                        cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());

                    bunch_update_unit.informCallersAboutPresentId(id, cell_idx);
                    /// mark corresponding id as found
                    remaining_ids[id] = 1;
                }
            }

            stream->readSuffix();

            /// Lock just for last_exception safety
            ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};

            error_count = 0;
            last_exception = std::exception_ptr{};
            backoff_end_time = std::chrono::system_clock::time_point{};

            ProfileEvents::increment(ProfileEvents::DictCacheRequestTimeNs, watch.elapsed());
        }
        catch (...)
        {
            /// Lock just for last_exception safety
            ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};
            ++error_count;
            last_exception = std::current_exception();
            backoff_end_time = now + std::chrono::seconds(calculateDurationWithBackoff(rnd_engine, error_count));

            tryLogException(last_exception, log, "Could not update cache dictionary '" + getFullName() +
                                                 "', next update is scheduled at " + ext::to_string(backoff_end_time.load()));
        }
    }

    /// Modifying cache state again with write lock
    ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};
    size_t not_found_num = 0;
    size_t found_num = 0;

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

        if (error_count)
        {
            if (find_result.outdated)
            {
                /// We have expired data for that `id` so we can continue using it.
                bool was_default = cell.isDefault();
                cell.setExpiresAt(backoff_end_time);
                if (was_default)
                    cell.setDefault();
                if (was_default)
                    bunch_update_unit.informCallersAboutAbsentId(id, cell_idx);
                else
                    bunch_update_unit.informCallersAboutPresentId(id, cell_idx);
                continue;
            }
            /// We don't have expired data for that `id` so all we can do is to rethrow `last_exception`.
            std::rethrow_exception(last_exception);
        }

        /// Check if cell had not been occupied before and increment element counter if it hadn't
        if (cell.id == 0 && cell_idx != zero_cell_idx)
            element_count.fetch_add(1, std::memory_order_relaxed);

        cell.id = id;

        if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
        {
            std::uniform_int_distribution<UInt64> distribution{dict_lifetime.min_sec, dict_lifetime.max_sec};
            cell.setExpiresAt(now + std::chrono::seconds{distribution(rnd_engine)});
            cell.strict_max = now + std::chrono::seconds{strict_max_lifetime_seconds};
        }
        else
        {
            cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());
            cell.strict_max = now + std::chrono::seconds{strict_max_lifetime_seconds};
        }


        /// Set null_value for each attribute
        cell.setDefault();
        for (auto & attribute : attributes)
            setDefaultAttributeValue(attribute, cell_idx);

        /// inform caller that the cell has not been found
        bunch_update_unit.informCallersAboutAbsentId(id, cell_idx);
    }

    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedMiss, not_found_num);
    ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedFound, found_num);
    ProfileEvents::increment(ProfileEvents::DictCacheRequests);
}

}
