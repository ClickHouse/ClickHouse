#include <DataTypes/StructuredSubstreamNames.h>

#include <optional>

#include <Common/escapeForFileName.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

namespace
{

using Substream = ISerialization::Substream;
using SubstreamPath = ISerialization::SubstreamPath;

bool isTupleOrVariantSubstream(Substream::Type type)
{
    return type == Substream::TupleElement
        || type == Substream::VariantElement
        || type == Substream::VariantElementNullMap
        || type == Substream::VariantDiscriminators
        || type == Substream::VariantDiscriminatorsPrefix
        || type == Substream::VariantOffsets;
}

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

/// Tuple / Variant path components in [begin_index, end_index) that disambiguate nested streams.
String getPathPrefixInRange(const SubstreamPath & path, size_t begin_index, size_t end_index)
{
    String stream_name;
    for (size_t i = begin_index; i < end_index && i < path.size(); ++i)
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

std::optional<size_t> findLastSubstreamInRange(
    const SubstreamPath & path, size_t begin_index, size_t end_index, Substream::Type type)
{
    std::optional<size_t> last_index;
    for (size_t i = begin_index; i < end_index && i < path.size(); ++i)
    {
        if (path[i].type == type)
            last_index = i;
    }
    return last_index;
}

String repeatArrayPrefix(size_t count)
{
    String result;
    for (size_t i = 0; i < count; ++i)
        result += ".array";
    return result;
}

size_t countLeadingTupleOrVariantPrefix(const SubstreamPath & path)
{
    size_t prefix_end = 0;
    while (prefix_end < path.size() && isTupleOrVariantSubstream(path[prefix_end].type))
        ++prefix_end;
    return prefix_end;
}

/// Build structured suffix for paths that contain Nullable(Array(...)) at any nesting depth.
String buildStructuredSubstreamNameSuffix(const SubstreamPath & path)
{
    if (path.empty())
        return "";

    const size_t path_size = path.size();
    const auto last_type = path.back().type;

    const size_t array_elements_count = countSubstreamsInRange(path, 0, path_size, Substream::ArrayElements);
    const bool has_array_sizes_before_end = pathContainsSubstreamInRange(path, 0, path_size, Substream::ArraySizes);
    const bool has_null_map_before_end = pathContainsSubstreamInRange(path, 0, path_size - 1, Substream::NullMap);

    if (last_type == Substream::NullMap)
    {
        if (array_elements_count == 0)
            return ".null";

        const bool is_element_null_map = array_elements_count >= 2 || has_array_sizes_before_end;
        if (is_element_null_map)
        {
            const auto last_array_elements = findLastSubstreamInRange(path, 0, path_size, Substream::ArrayElements);
            const String element_context = last_array_elements
                ? getPathPrefixInRange(path, *last_array_elements + 1, path_size - 1)
                : String{};
            return repeatArrayPrefix(array_elements_count) + ".nested" + element_context + ".null";
        }

        return ".array.null";
    }

    if (last_type == Substream::ArraySizes)
    {
        const size_t array_sizes_count = countSubstreamsInRange(path, 0, path_size, Substream::ArraySizes);

        if (array_elements_count == 0)
        {
            if (has_null_map_before_end)
                return ".array.size" + toString(array_sizes_count - 1);
            return ".size" + toString(array_sizes_count - 1);
        }

        const auto last_array_elements = findLastSubstreamInRange(path, 0, path_size, Substream::ArrayElements);
        const size_t array_sizes_after_last_elements = last_array_elements
            ? countSubstreamsInRange(path, *last_array_elements + 1, path_size, Substream::ArraySizes)
            : array_sizes_count;

        return repeatArrayPrefix(array_elements_count) + ".array.size" + toString(array_sizes_after_last_elements - 1);
    }

    if (last_type == Substream::Regular)
    {
        if (path_size >= 2 && path[path_size - 2].type == Substream::ArrayElements)
            return ".array.nested";

        const auto last_array_elements = findLastSubstreamInRange(path, 0, path_size, Substream::ArrayElements);
        if (!last_array_elements)
            return "";

        const String element_context = getPathPrefixInRange(path, *last_array_elements + 1, path_size - 1);
        const bool is_element_data = array_elements_count >= 2 || has_array_sizes_before_end;

        if (is_element_data)
            return repeatArrayPrefix(array_elements_count) + ".nested" + element_context;

        return ".array.nested" + element_context;
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

    if (const auto * map = typeid_cast<const DataTypeMap *>(&type))
    {
        if (needsStructuredSubstreamNames(*map->getKeyType()) || needsStructuredSubstreamNames(*map->getValueType()))
            return true;
    }

    if (const auto * variant = typeid_cast<const DataTypeVariant *>(&type))
    {
        for (const auto & alternative : variant->getVariants())
        {
            if (needsStructuredSubstreamNames(*alternative))
                return true;
        }
    }

    return false;
}

String getStructuredSubstreamNameSuffix(const SubstreamPath & path)
{
    const size_t leading_prefix_end = countLeadingTupleOrVariantPrefix(path);
    String result = getPathPrefixInRange(path, 0, leading_prefix_end);
    result += buildStructuredSubstreamNameSuffix(path);

    if (!result.empty())
        return result;

    return getLegacySubstreamNameSuffix(path.begin(), path.end(), false, true);
}

}
