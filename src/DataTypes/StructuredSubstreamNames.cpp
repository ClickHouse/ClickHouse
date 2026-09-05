#include <DataTypes/StructuredSubstreamNames.h>

#include <optional>

#include <Common/escapeForFileName.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeVariant.h>
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
        if (path[i].type == type || (type == Substream::NullMap && path[i].type == Substream::NullMapHidden))
            return true;
    }
    return false;
}

size_t countSubstreamsInRange(const SubstreamPath & path, size_t begin, size_t end, Substream::Type type)
{
    size_t count = 0;
    for (size_t i = begin; i < end && i < path.size(); ++i)
    {
        if (path[i].type == type || (type == Substream::NullMap && path[i].type == Substream::NullMapHidden))
            ++count;
    }
    return count;
}

/// The name fragment contributed by the path components in [begin_index, end_index) that
/// disambiguate nested streams - tuple and variant elements, object and dynamic paths, and so on.
///
/// Delegates to `ISerialization` per element instead of restating the substream-to-name mapping.
/// The substreams whose naming the structured scheme actually changes - array elements, array sizes,
/// null maps and the terminal value - are handled by the callers and never reach here, so the shared
/// mapping is exactly the right answer for everything that does.
String getPathPrefixInRange(const SubstreamPath & path, size_t begin_index, size_t end_index)
{
    String stream_name;
    for (size_t i = begin_index; i < end_index && i < path.size(); ++i)
    {
        SubstreamPath element_path;
        element_path.push_back(path[i]);
        stream_name += ISerialization::getLegacyNameForSubstreamPath(
            element_path, /*escape_for_file_name=*/true, /*encode_sparse_stream=*/false, /*escape_variant_substreams=*/true);
    }
    return stream_name;
}

String getStructuredPathPrefixInRange(const SubstreamPath & path, size_t begin_index, size_t end_index)
{
    String stream_name;
    for (size_t i = begin_index; i < end_index && i < path.size(); ++i)
    {
        if (path[i].type == Substream::ArrayElements)
            stream_name += ".array";
        else
            stream_name += getPathPrefixInRange(path, i, i + 1);
    }
    return stream_name;
}

std::optional<size_t> findLastSubstreamInRange(
    const SubstreamPath & path, size_t begin_index, size_t end_index, Substream::Type type)
{
    std::optional<size_t> last_index;
    for (size_t i = begin_index; i < end_index && i < path.size(); ++i)
    {
        if (path[i].type == type || (type == Substream::NullMap && path[i].type == Substream::NullMapHidden))
            last_index = i;
    }
    return last_index;
}

/// Build structured suffix for paths that contain Nullable(Array(...)) at any nesting depth.
String buildStructuredSubstreamNameSuffix(const SubstreamPath & path)
{
    if (path.empty())
        return "";

    const size_t path_size = path.size();
    const auto last_type = path.back().type;

    if (last_type == Substream::Regular && path_size >= 2)
    {
        const auto named_subcolumn_index = path_size - 2;
        const auto named_subcolumn_type = path[named_subcolumn_index].type;
        if (named_subcolumn_type == Substream::NamedOffsets || named_subcolumn_type == Substream::NamedNullMap)
        {
            SubstreamPath storage_path = path;
            storage_path.resize(named_subcolumn_index + 1);
            storage_path.back() = Substream(named_subcolumn_type == Substream::NamedOffsets ? Substream::ArraySizes : Substream::NullMap);
            return buildStructuredSubstreamNameSuffix(storage_path);
        }
    }

    const size_t array_elements_count = countSubstreamsInRange(path, 0, path_size, Substream::ArrayElements);
    const bool has_array_sizes_before_end = pathContainsSubstreamInRange(path, 0, path_size, Substream::ArraySizes);
    const bool has_null_map_before_end = pathContainsSubstreamInRange(path, 0, path_size - 1, Substream::NullMap);

    if (last_type == Substream::NullMap || last_type == Substream::NullMapHidden)
    {
        String path_context = getStructuredPathPrefixInRange(path, 0, path_size - 1);

        if (array_elements_count == 0)
            return path_context + ".null";

        const bool is_element_null_map = array_elements_count >= 2 || has_array_sizes_before_end;
        if (is_element_null_map)
            return path_context + ".nested.null";

        return path_context + ".null";
    }

    if (last_type == Substream::ArraySizes)
    {
        const size_t array_sizes_count = countSubstreamsInRange(path, 0, path_size, Substream::ArraySizes);
        const String path_context = getStructuredPathPrefixInRange(path, 0, path_size - 1);

        if (array_elements_count == 0)
        {
            if (has_null_map_before_end)
                return path_context + ".array.size" + toString(array_sizes_count - 1);
            return path_context + ".size" + toString(array_sizes_count - 1);
        }

        const auto last_array_elements = findLastSubstreamInRange(path, 0, path_size, Substream::ArrayElements);
        const size_t array_sizes_after_last_elements = last_array_elements
            ? countSubstreamsInRange(path, *last_array_elements + 1, path_size, Substream::ArraySizes)
            : array_sizes_count;

        return path_context + ".array.size" + toString(array_sizes_after_last_elements - 1);
    }

    if (last_type == Substream::Regular)
    {
        String path_context = getStructuredPathPrefixInRange(path, 0, path_size - 1);
        if (array_elements_count == 0)
            return path_context;

        return path_context + ".nested";
    }

    return "";
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

    if (const auto * object = typeid_cast<const DataTypeObject *>(&type))
    {
        for (const auto & [path_name, path_type] : object->getTypedPaths())
        {
            if (needsStructuredSubstreamNames(*path_type))
                return true;
        }
    }

    return false;
}


/// Check if a substream path contains a Nullable(Array(...)) pattern that requires
/// structured naming, even when the static column type doesn't reveal it. This only
/// applies to Dynamic and Object columns whose runtime/typed-path types are not visible
/// in the static column type. For ordinary Array(Nullable(T)) columns the static type
/// already returns false from needsStructuredSubstreamNames, and we must keep legacy
/// naming to preserve compatibility with existing MergeTree parts.
bool needsStructuredSubstreamNamesForPath(const SubstreamPath & path)
{
    /// Covers `Dynamic` and `Object` columns, whose static type does not reveal that a runtime variant
    /// holds a `Nullable(Array)`.
    ///
    /// The shape is a `Nullable` sitting *directly* on an `Array`: `SerializationNullable` pushes
    /// `NullableElements` and `SerializationArray` pushes an array substream immediately after it.
    /// Adjacency is the whole point. `Array(Nullable(T))` puts the same two components on a path in
    /// the opposite nesting, and `Nullable(Tuple(x Array(T)))` puts a `TupleElement` between them -
    /// both are ordinary types that must keep their legacy names.
    for (size_t i = 0; i + 1 < path.size(); ++i)
    {
        if (path[i].type != Substream::NullableElements)
            continue;

        const auto next = path[i + 1].type;
        if (next != Substream::ArraySizes && next != Substream::ArrayElements)
            continue;

        for (size_t j = 0; j < i; ++j)
        {
            if (path[j].type == Substream::DynamicData
                || path[j].type == Substream::ObjectTypedPath
                || path[j].type == Substream::ObjectDynamicPath)
                return true;
        }
    }
    return false;
}

String getStructuredSubstreamNameSuffix(const SubstreamPath & path)
{
    String result = buildStructuredSubstreamNameSuffix(path);

    if (!result.empty())
        return result;

    /// Substreams that the structured scheme does not rename keep their legacy names. Delegating to
    /// `ISerialization` rather than re-implementing that mapping here is deliberate: a local copy of it
    /// had already lost `MapBucketIndexes`, which would have made a bucketed `Map` holding a
    /// `Nullable(Array)` write two different streams under one file name.
    return ISerialization::getLegacyNameForSubstreamPath(
        path, /*escape_for_file_name=*/true, /*encode_sparse_stream=*/false, /*escape_variant_substreams=*/true);
}

}
