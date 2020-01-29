#include "DirectDictionary.h"

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
#include "DirectDictionary.inc.h"
#include "DictionaryBlockInputStream.h"
#include "DictionaryFactory.h"


/*
 *
 * TODO: CHANGE EVENTS TO DIRECT DICTIONARY EVENTS (WTF? WHERE R THEY DECLARED????)
 *
*/

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

    /*
     * deleted inline size_t DirectDictionary::getCellIdx(const Key id) const
     *
     */


    DirectDictionary::DirectDictionary(
            const std::string & name_,
            const DictionaryStructure & dict_struct_,
            DictionarySourcePtr source_ptr_,
            const DictionaryLifetime dict_lifetime_
            )
            : name{name_}
            , dict_struct(dict_struct_)
            , source_ptr{std::move(source_ptr_)}
            , dict_lifetime(dict_lifetime_)
            , log(&Logger::get("ExternalDictionaries"))
            , rnd_engine(randomSeed())
    {
        if (!this->source_ptr->supportsSelectiveLoad())
            throw Exception{name + ": source cannot be used with DirectDictionary", ErrorCodes::UNSUPPORTED_METHOD};

        createAttributes();
    }


    void DirectDictionary::toParent(const PaddedPODArray<Key> & ids, PaddedPODArray<Key> & out) const
    {
        const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);

        getItemsNumberImpl<UInt64, UInt64>(*hierarchical_attribute, ids, out, [&](const size_t) { return null_value; });
    }


/// Allow to use single value in same way as array.
    static inline DirectDictionary::Key getAt(const PaddedPODArray<DirectDictionary::Key> & arr, const size_t idx)
    {
        return arr[idx];
    }
    static inline DirectDictionary::Key getAt(const DirectDictionary::Key & value, const size_t)
    {
        return value;
    }


    template <typename AncestorType>
    void DirectDictionary::isInImpl(const PaddedPODArray<Key> & child_ids, const AncestorType & ancestor_ids, PaddedPODArray<UInt8> & out) const
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

    void DirectDictionary::isInVectorVector(
            const PaddedPODArray<Key> & child_ids, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const
    {
        isInImpl(child_ids, ancestor_ids, out);
    }

    void DirectDictionary::isInVectorConstant(const PaddedPODArray<Key> & child_ids, const Key ancestor_id, PaddedPODArray<UInt8> & out) const
    {
        isInImpl(child_ids, ancestor_id, out);
    }

    void DirectDictionary::isInConstantVector(const Key child_id, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const
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

    void DirectDictionary::getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ColumnString * out) const
    {
        auto & attribute = getAttribute(attribute_name);
        checkAttributeType(name, attribute_name, attribute.type, AttributeUnderlyingType::utString);

        const auto null_value = StringRef{std::get<String>(attribute.null_values)};

        getItemsString(attribute, ids, out, [&](const size_t) { return null_value; });
    }

    void DirectDictionary::getString(
            const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def, ColumnString * const out) const
    {
        auto & attribute = getAttribute(attribute_name);
        checkAttributeType(name, attribute_name, attribute.type, AttributeUnderlyingType::utString);

        getItemsString(attribute, ids, out, [&](const size_t row) { return def->getDataAt(row); });
    }

    void DirectDictionary::getString(
            const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def, ColumnString * const out) const
    {
        auto & attribute = getAttribute(attribute_name);
        checkAttributeType(name, attribute_name, attribute.type, AttributeUnderlyingType::utString);

        getItemsString(attribute, ids, out, [&](const size_t) { return StringRef{def}; });
    }


/// returns cell_idx (always valid for replacing), 'cell is valid' flag, 'cell is outdated' flag
/// true  false   found and valid
/// false true    not found (something outdated, maybe our cell)
/// false false   not found (other id stored with valid data)
/// true  true    impossible
///
/// todo: split this func to two: find_for_get and find_for_set
    DirectDictionary::FindResult DirectDictionary::findCellIdx(const Key & id, const CellMetadata::time_point_t now) const
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


    /*
     * deleted most part of has, that stood for
     * looking for a key in cache
     *
     * TODO: check whether we need last two arguments
     * in update function (seems like no)
     *
     */

    void DirectDictionary::has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const
    {
        std::vector<Key> required_ids(ids.size());
        std::copy(std::begin(ids), std::end(ids), std::begin(required_ids));

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


    void DirectDictionary::createAttributes()
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
                    throw Exception{name + ": hierarchical attribute must be UInt64.", ErrorCodes::TYPE_MISMATCH};
            }
        }
    }

    DirectDictionary::Attribute DirectDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
    {
        Attribute attr{type, {}, {}};

        switch (type)
        {
#define DISPATCH(TYPE) \
    case AttributeUnderlyingType::ut##TYPE: \
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

    void DirectDictionary::setDefaultAttributeValue(Attribute & attribute, const Key idx) const
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

    void DirectDictionary::setAttributeValue(Attribute & attribute, const Key idx, const Field & value) const
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

    DirectDictionary::Attribute & DirectDictionary::getAttribute(const std::string & attribute_name) const
    {
        const auto it = attribute_index_by_name.find(attribute_name);
        if (it == std::end(attribute_index_by_name))
            throw Exception{name + ": no such attribute '" + attribute_name + "'", ErrorCodes::BAD_ARGUMENTS};

        return attributes[it->second];
    }

    /*
     * I've deleted:
     * bool CacheDictionary::isEmptyCell(const UInt64 idx) const
     * and
     * PaddedPODArray<CacheDictionary::Key> CacheDictionary::getCachedIds() const
     */

    BlockInputStreamPtr DirectDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
    {
        using BlockInputStreamType = DictionaryBlockInputStream<DirectDictionary, Key>;

        /* deleted pre-last argument getCachedIds() from this return (will something break then?) */
        return std::make_shared<BlockInputStreamType>(shared_from_this(), max_block_size, column_names);
    }

    std::exception_ptr DirectDictionary::getLastException() const
    {
        const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};
        return last_exception;
    }

    void registerDictionaryDirect(DictionaryFactory & factory)
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

            /*
             *
             * seems like this stands only for cache dictionaries
             *
            const auto size = config.getInt(layout_prefix + ".cache.size_in_cells");
            if (size == 0)
            throw Exception{name + ": dictionary of layout 'cache' cannot have 0 cells", ErrorCodes::TOO_SMALL_BUFFER_SIZE};

             */

            const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);
            if (require_nonempty)
                throw Exception{name + ": dictionary of layout 'cache' cannot have 'require_nonempty' attribute set",
                                ErrorCodes::BAD_ARGUMENTS};

            const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

            /* deleted last argument (size) in this return */
            return std::make_unique<DirectDictionary>(name, dict_struct, std::move(source_ptr), dict_lifetime);
        };
        factory.registerLayout("direct", create_layout, false);
    }


}
