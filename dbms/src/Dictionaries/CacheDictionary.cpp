#include "CacheDictionary.h"

#include <functional>
#include <memory>
#include <sstream>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/BitHelpers.h>
#include <Common/CurrentMetrics.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <Common/ProfilingScopedRWLock.h>
#include <Common/Stopwatch.h>
#include <Common/randomSeed.h>
#include <Common/typeid_cast.h>
#include <ext/map.h>
#include <ext/range.h>
#include <ext/size.h>
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
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int TOO_SMALL_BUFFER_SIZE;
}


inline size_t CacheDictionary::getCellIdx(const Key id) const
{
    const auto hash = intHash64(id);
    const auto idx = hash & size_overlap_mask;
    return idx;
}


CacheDictionary::CacheDictionary(
    const std::string & name,
    const DictionaryStructure & dict_struct,
    DictionarySourcePtr source_ptr,
    const DictionaryLifetime dict_lifetime,
    const size_t size)
    : name{name}
    , dict_struct(dict_struct)
    , source_ptr{std::move(source_ptr)}
    , dict_lifetime(dict_lifetime)
    , size{roundUpToPowerOfTwoOrZero(std::max(size, size_t(max_collision_length)))}
    , size_overlap_mask{this->size - 1}
    , cells{this->size}
    , rnd_engine(randomSeed())
{
    if (!this->source_ptr->supportsSelectiveLoad())
        throw Exception{name + ": source cannot be used with CacheDictionary", ErrorCodes::UNSUPPORTED_METHOD};

    createAttributes();
}


void CacheDictionary::toParent(const PaddedPODArray<Key> & ids, PaddedPODArray<Key> & out) const
{
    const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);

    getItemsNumber<UInt64>(*hierarchical_attribute, ids, out, [&](const size_t) { return null_value; });
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

void CacheDictionary::getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ColumnString * out) const
{
    auto & attribute = getAttribute(attribute_name);
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
        throw Exception{name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
                        ErrorCodes::TYPE_MISMATCH};

    const auto null_value = StringRef{std::get<String>(attribute.null_values)};

    getItemsString(attribute, ids, out, [&](const size_t) { return null_value; });
}

void CacheDictionary::getString(
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def, ColumnString * const out) const
{
    auto & attribute = getAttribute(attribute_name);
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
        throw Exception{name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
                        ErrorCodes::TYPE_MISMATCH};

    getItemsString(attribute, ids, out, [&](const size_t row) { return def->getDataAt(row); });
}

void CacheDictionary::getString(
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def, ColumnString * const out) const
{
    auto & attribute = getAttribute(attribute_name);
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
        throw Exception{name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
                        ErrorCodes::TYPE_MISMATCH};

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
    std::transform(std::begin(outdated_ids), std::end(outdated_ids), std::begin(required_ids), [](auto & pair) { return pair.first; });

    /// request new values
    update(
        required_ids,
        [&](const auto id, const auto)
        {
            for (const auto row : outdated_ids[id])
                out[row] = true;
        },
        [&](const auto id, const auto)
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
#define DISPATCH(TYPE) \
    case AttributeUnderlyingType::TYPE: \
        attr.null_values = TYPE(null_value.get<NearestFieldType<TYPE>>()); \
        attr.arrays = std::make_unique<ContainerType<TYPE>>(size); \
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
        case AttributeUnderlyingType::String:
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
        case AttributeUnderlyingType::UInt8:
            std::get<ContainerPtrType<UInt8>>(attribute.arrays)[idx] = std::get<UInt8>(attribute.null_values);
            break;
        case AttributeUnderlyingType::UInt16:
            std::get<ContainerPtrType<UInt16>>(attribute.arrays)[idx] = std::get<UInt16>(attribute.null_values);
            break;
        case AttributeUnderlyingType::UInt32:
            std::get<ContainerPtrType<UInt32>>(attribute.arrays)[idx] = std::get<UInt32>(attribute.null_values);
            break;
        case AttributeUnderlyingType::UInt64:
            std::get<ContainerPtrType<UInt64>>(attribute.arrays)[idx] = std::get<UInt64>(attribute.null_values);
            break;
        case AttributeUnderlyingType::UInt128:
            std::get<ContainerPtrType<UInt128>>(attribute.arrays)[idx] = std::get<UInt128>(attribute.null_values);
            break;
        case AttributeUnderlyingType::Int8:
            std::get<ContainerPtrType<Int8>>(attribute.arrays)[idx] = std::get<Int8>(attribute.null_values);
            break;
        case AttributeUnderlyingType::Int16:
            std::get<ContainerPtrType<Int16>>(attribute.arrays)[idx] = std::get<Int16>(attribute.null_values);
            break;
        case AttributeUnderlyingType::Int32:
            std::get<ContainerPtrType<Int32>>(attribute.arrays)[idx] = std::get<Int32>(attribute.null_values);
            break;
        case AttributeUnderlyingType::Int64:
            std::get<ContainerPtrType<Int64>>(attribute.arrays)[idx] = std::get<Int64>(attribute.null_values);
            break;
        case AttributeUnderlyingType::Float32:
            std::get<ContainerPtrType<Float32>>(attribute.arrays)[idx] = std::get<Float32>(attribute.null_values);
            break;
        case AttributeUnderlyingType::Float64:
            std::get<ContainerPtrType<Float64>>(attribute.arrays)[idx] = std::get<Float64>(attribute.null_values);
            break;

        case AttributeUnderlyingType::Decimal32:
            std::get<ContainerPtrType<Decimal32>>(attribute.arrays)[idx] = std::get<Decimal32>(attribute.null_values);
            break;
        case AttributeUnderlyingType::Decimal64:
            std::get<ContainerPtrType<Decimal64>>(attribute.arrays)[idx] = std::get<Decimal64>(attribute.null_values);
            break;
        case AttributeUnderlyingType::Decimal128:
            std::get<ContainerPtrType<Decimal128>>(attribute.arrays)[idx] = std::get<Decimal128>(attribute.null_values);
            break;

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
        case AttributeUnderlyingType::UInt8:
            std::get<ContainerPtrType<UInt8>>(attribute.arrays)[idx] = value.get<UInt64>();
            break;
        case AttributeUnderlyingType::UInt16:
            std::get<ContainerPtrType<UInt16>>(attribute.arrays)[idx] = value.get<UInt64>();
            break;
        case AttributeUnderlyingType::UInt32:
            std::get<ContainerPtrType<UInt32>>(attribute.arrays)[idx] = value.get<UInt64>();
            break;
        case AttributeUnderlyingType::UInt64:
            std::get<ContainerPtrType<UInt64>>(attribute.arrays)[idx] = value.get<UInt64>();
            break;
        case AttributeUnderlyingType::UInt128:
            std::get<ContainerPtrType<UInt128>>(attribute.arrays)[idx] = value.get<UInt128>();
            break;
        case AttributeUnderlyingType::Int8:
            std::get<ContainerPtrType<Int8>>(attribute.arrays)[idx] = value.get<Int64>();
            break;
        case AttributeUnderlyingType::Int16:
            std::get<ContainerPtrType<Int16>>(attribute.arrays)[idx] = value.get<Int64>();
            break;
        case AttributeUnderlyingType::Int32:
            std::get<ContainerPtrType<Int32>>(attribute.arrays)[idx] = value.get<Int64>();
            break;
        case AttributeUnderlyingType::Int64:
            std::get<ContainerPtrType<Int64>>(attribute.arrays)[idx] = value.get<Int64>();
            break;
        case AttributeUnderlyingType::Float32:
            std::get<ContainerPtrType<Float32>>(attribute.arrays)[idx] = value.get<Float64>();
            break;
        case AttributeUnderlyingType::Float64:
            std::get<ContainerPtrType<Float64>>(attribute.arrays)[idx] = value.get<Float64>();
            break;

        case AttributeUnderlyingType::Decimal32:
            std::get<ContainerPtrType<Decimal32>>(attribute.arrays)[idx] = value.get<Decimal32>();
            break;
        case AttributeUnderlyingType::Decimal64:
            std::get<ContainerPtrType<Decimal64>>(attribute.arrays)[idx] = value.get<Decimal64>();
            break;
        case AttributeUnderlyingType::Decimal128:
            std::get<ContainerPtrType<Decimal128>>(attribute.arrays)[idx] = value.get<Decimal128>();
            break;

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

void registerDictionaryCache(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        if (dict_struct.key)
            throw Exception{"'key' is not supported for dictionary of layout 'cache'", ErrorCodes::UNSUPPORTED_METHOD};

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception{name
                                + ": elements .structure.range_min and .structure.range_max should be defined only "
                                  "for a dictionary of layout 'range_hashed'",
                            ErrorCodes::BAD_ARGUMENTS};
        const auto & layout_prefix = config_prefix + ".layout";
        const auto size = config.getInt(layout_prefix + ".cache.size_in_cells");
        if (size == 0)
            throw Exception{name + ": dictionary of layout 'cache' cannot have 0 cells", ErrorCodes::TOO_SMALL_BUFFER_SIZE};

        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);
        if (require_nonempty)
            throw Exception{name + ": dictionary of layout 'cache' cannot have 'require_nonempty' attribute set",
                            ErrorCodes::BAD_ARGUMENTS};

        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        return std::make_unique<CacheDictionary>(name, dict_struct, std::move(source_ptr), dict_lifetime, size);
    };
    factory.registerLayout("cache", create_layout);
}


}
