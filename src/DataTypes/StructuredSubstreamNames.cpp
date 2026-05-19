#include <DataTypes/StructuredSubstreamNames.h>

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

bool pathContainsSubstream(const SubstreamPath & path, Substream::Type type)
{
    for (const auto & element : path)
    {
        if (element.type == type)
            return true;
    }
    return false;
}

size_t countSubstreamsBefore(const SubstreamPath & path, size_t end_index, Substream::Type type)
{
    size_t count = 0;
    for (size_t i = 0; i < end_index && i < path.size(); ++i)
    {
        if (path[i].type == type)
            ++count;
    }
    return count;
}

bool isOuterNullableArrayNullMap(const SubstreamPath & path)
{
    return path.size() == 1 && path[0].type == Substream::NullMap;
}

bool isArraySizeInNullableArray(const SubstreamPath & path)
{
    if (path.empty() || path.back().type != Substream::ArraySizes)
        return false;

    return path.size() >= 3
        && path[0].type == Substream::NullMap
        && path[1].type == Substream::NullableElements;
}

bool isElementNullMapInNullableArray(const SubstreamPath & path)
{
    if (path.empty() || path.back().type != Substream::NullMap)
        return false;

    return pathContainsSubstream(path, Substream::ArrayElements);
}

bool isElementDataInNullableArray(const SubstreamPath & path)
{
    if (path.size() < 2)
        return false;

    return path[path.size() - 1].type == Substream::Regular
        && path[path.size() - 2].type == Substream::NullableElements
        && pathContainsSubstream(path, Substream::ArrayElements);
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
    if (isOuterNullableArrayNullMap(path))
        return ".null";

    if (isArraySizeInNullableArray(path))
    {
        const size_t array_level = countSubstreamsBefore(path, path.size(), Substream::ArraySizes) - 1;
        return ".array.size" + toString(array_level);
    }

    if (isElementNullMapInNullableArray(path))
        return ".array.nested.null";

    if (isElementDataInNullableArray(path))
        return ".array.nested";

    return getLegacySubstreamNameSuffix(path.begin(), path.end(), false, true);
}

}
