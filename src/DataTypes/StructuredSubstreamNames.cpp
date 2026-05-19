#include <DataTypes/StructuredSubstreamNames.h>

#include <optional>

#include <Common/escapeForFileName.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

namespace
{

using Substream = ISerialization::Substream;
using SubstreamPath = ISerialization::SubstreamPath;

bool pathContainsSubstreamInRange(const SubstreamPath & path, size_t begin, size_t end, Substream::Type type)
{
    for (size_t i = begin; i < end && i < path.size(); ++i)
    {
        if (path[i].type == type)
            return true;
    }
    return false;
}

size_t countSubstreamsInRange(const SubstreamPath & path, size_t begin, size_t end, Substream::Type type)
{
    size_t count = 0;
    for (size_t i = begin; i < end && i < path.size(); ++i)
    {
        if (path[i].type == type)
            ++count;
    }
    return count;
}

/// Tuple / Variant path components that must stay in the suffix to disambiguate columns.
String getPathPrefixBeforeNullableArray(const SubstreamPath & path, size_t end_index)
{
    String stream_name;
    for (size_t i = 0; i < end_index && i < path.size(); ++i)
    {
        const auto & element = path[i];
        if (element.type == Substream::TupleElement)
            stream_name += escapeForFileName("." + element.name_of_substream);
        else if (element.type == Substream::VariantElement)
            stream_name += "." + escapeForFileName(element.variant_element_name);
        else if (element.type == Substream::VariantElementNullMap)
            stream_name += "." + escapeForFileName(element.variant_element_name) + ".null";
        else if (element.type == Substream::VariantDiscriminators)
            stream_name += ".variant_discr";
        else if (element.type == Substream::VariantDiscriminatorsPrefix)
            stream_name += ".variant_discr_prefix";
        else if (element.type == Substream::VariantOffsets)
            stream_name += ".variant_offsets";
    }
    return stream_name;
}

/// Index of the outer Nullable(Array) null map: first NullMap not preceded by ArrayElements.
std::optional<size_t> findOuterNullableArrayNullMapIndex(const SubstreamPath & path)
{
    for (size_t i = 0; i < path.size(); ++i)
    {
        if (path[i].type != Substream::NullMap)
            continue;

        bool has_array_elements_before = false;
        for (size_t j = 0; j < i; ++j)
        {
            if (path[j].type == Substream::ArrayElements)
            {
                has_array_elements_before = true;
                break;
            }
        }

        if (!has_array_elements_before)
            return i;
    }

    return std::nullopt;
}

/// Structured suffix for the Nullable(Array) portion of the path (from outer null map onward).
String getNullableArrayStructuredSuffix(const SubstreamPath & path, size_t nullable_array_start)
{
    const size_t path_size = path.size();
    if (nullable_array_start >= path_size)
        return "";

    const size_t nullable_path_len = path_size - nullable_array_start;

    if (nullable_path_len == 1 && path[nullable_array_start].type == Substream::NullMap)
        return ".null";

    if (path_size >= 2
        && path[path_size - 1].type == Substream::ArraySizes
        && path[path_size - 2].type == Substream::NullableElements)
    {
        const size_t array_sizes_in_path = countSubstreamsInRange(path, nullable_array_start, path_size, Substream::ArraySizes);
        return ".array.size" + toString(array_sizes_in_path - 1);
    }

    if (path_size >= 2
        && path[path_size - 1].type == Substream::NullMap
        && pathContainsSubstreamInRange(path, nullable_array_start, path_size, Substream::ArrayElements))
    {
        return ".array.nested.null";
    }

    if (path_size >= 2 && path[path_size - 1].type == Substream::Regular)
    {
        if (path[path_size - 2].type == Substream::ArrayElements)
            return ".array.nested";

        if (path_size >= 4
            && path[path_size - 2].type == Substream::NullableElements
            && path[path_size - 3].type == Substream::NullMap
            && path[path_size - 4].type == Substream::ArrayElements)
        {
            return ".array.nested";
        }
    }

    return "";
}

String getLegacySubstreamNameSuffix(
    SubstreamPath::const_iterator begin,
    SubstreamPath::const_iterator end,
    bool encode_sparse_stream,
    bool escape_variant_substreams)
{
    String stream_name;
    size_t array_level = 0;

    for (auto it = begin; it != end; ++it)
    {
        if (it->type == Substream::NullMap || it->type == Substream::SparseNullMap)
            stream_name += ".null";
        else if (it->type == Substream::ArraySizes)
            stream_name += ".size" + toString(array_level);
        else if (it->type == Substream::ArrayElements)
            ++array_level;
        else if (it->type == Substream::StringSizes || it->type == Substream::InlinedStringSizes)
            stream_name += ".size";
        else if (it->type == Substream::DictionaryKeys)
            stream_name += ".dict";
        else if (it->type == Substream::DictionaryKeysPrefix)
            stream_name += ".dict_prefix";
        else if (it->type == Substream::SparseElements && encode_sparse_stream)
            stream_name += ".sparse";
        else if (it->type == Substream::SparseOffsets)
            stream_name += ".sparse.idx";
        else if (it->type == Substream::ReplicatedElements)
            stream_name += ".repl";
        else if (it->type == Substream::ReplicatedIndexes)
            stream_name += ".repl.idx";
        else if (Substream::named_types.contains(it->type))
        {
            auto substream_name = "." + it->name_of_substream;
            if (it->type == Substream::TupleElement)
                stream_name += escapeForFileName(substream_name);
            else
                stream_name += substream_name;
        }
        else if (it->type == Substream::VariantDiscriminators)
            stream_name += ".variant_discr";
        else if (it->type == Substream::VariantDiscriminatorsPrefix)
            stream_name += ".variant_discr_prefix";
        else if (it->type == Substream::VariantOffsets)
            stream_name += ".variant_offsets";
        else if (it->type == Substream::VariantElement)
        {
            if (escape_variant_substreams)
                stream_name += "." + escapeForFileName(it->variant_element_name);
            else
                stream_name += "." + it->variant_element_name;
        }
        else if (it->type == Substream::VariantElementNullMap)
        {
            if (escape_variant_substreams)
                stream_name += "." + escapeForFileName(it->variant_element_name) + ".null";
            else
                stream_name += "." + it->variant_element_name + ".null";
        }
    }

    return stream_name;
}

}

bool needsStructuredSubstreamNames(const IDataType & type)
{
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(&type))
    {
        if (typeid_cast<const DataTypeArray *>(nullable->getNestedType().get()))
            return true;
        return needsStructuredSubstreamNames(*nullable->getNestedType());
    }

    if (const auto * array = typeid_cast<const DataTypeArray *>(&type))
        return needsStructuredSubstreamNames(*array->getNestedType());

    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(&type))
    {
        for (const auto & element : tuple->getElements())
        {
            if (needsStructuredSubstreamNames(*element))
                return true;
        }
    }

    return false;
}

String getStructuredSubstreamNameSuffix(const SubstreamPath & path)
{
    const auto outer_null_map_index = findOuterNullableArrayNullMapIndex(path);
    if (outer_null_map_index)
    {
        String result = getPathPrefixBeforeNullableArray(path, *outer_null_map_index);
        result += getNullableArrayStructuredSuffix(path, *outer_null_map_index);
        if (!result.empty())
            return result;
    }

    return getLegacySubstreamNameSuffix(path.begin(), path.end(), false, true);
}

}
