#include <Storages/SubcolumnCodecs.h>

#include <Compression/CompressionFactory.h>
#include <Compression/ICompressionCodec.h>
#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <Storages/ColumnDefault.h>
#include <Storages/ColumnsDescription.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// An ALIAS column has no streams, so it cannot have codecs.
void checkAliasColumnHasNoCodecs(const ColumnDescription & column)
{
    if (column.default_desc.kind == ColumnDefaultKind::Alias && (column.codec || !column.subcolumn_codecs.empty()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column {} is ALIAS, so it cannot have codecs", backQuote(column.name));
}

/// A column codec covers every stream of the column, so it excludes codecs for subcolumns.
void checkNoColumnCodecWithSubcolumnCodecs(const ColumnDescription & column)
{
    if (column.codec && !column.subcolumn_codecs.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Column {} has a codec, so codecs cannot be declared for its subcolumns", backQuote(column.name));
}

/// A declared subcolumn cannot be inside another declared subcolumn. The ancestors are the prefixes of the path
/// of the subcolumn, not of its name: an element named `a.b` is a sibling of an element named `a`.
void checkSubcolumnCodecsDoNotNest(
    const ColumnDescription & column, const String & subcolumn_name, const ISerialization::SubstreamPath & subcolumn_path)
{
    for (size_t prefix_len = 1; prefix_len < subcolumn_path.size(); ++prefix_len)
    {
        const String ancestor_name = ISerialization::getSubcolumnNameForStream(subcolumn_path, prefix_len);
        if (ancestor_name != subcolumn_name && column.subcolumn_codecs.contains(ancestor_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Codecs are declared for both subcolumn {} and subcolumn {} of column {}, but the second one is inside the first one",
                backQuote(ancestor_name), backQuote(subcolumn_name), backQuote(column.name));
    }
}

/// True when the subcolumn is inside a `JSON`, `Dynamic` or `Variant` value: its path goes through a substream of one of them.
bool isInsideDynamicType(const ISerialization::SubstreamPath & substream_path)
{
    using Substream = ISerialization::Substream;
    for (const auto & elem : substream_path)
    {
        switch (elem.type)
        {
            case Substream::ObjectData:
            case Substream::ObjectStructure:
            case Substream::ObjectTypedPath:
            case Substream::ObjectDynamicPath:
            case Substream::ObjectSharedData:
            case Substream::DynamicData:
            case Substream::DynamicStructure:
            case Substream::VariantDiscriminators:
            case Substream::VariantOffsets:
            case Substream::VariantElements:
            case Substream::VariantElement:
            case Substream::VariantElementNullMap:
                return true;
            default:
                break;
        }
    }
    return false;
}

/// True when the subcolumn is the keys of a `Map` or something inside them.
bool isInsideMapKeys(const ISerialization::SubstreamPath & substream_path)
{
    ISerialization::SubstreamPath prefix;
    for (const auto & elem : substream_path)
    {
        prefix.push_back(elem);
        if (SerializationMap::isKeysSubcolumn(prefix))
            return true;
    }
    return false;
}

/// True when `codec`, applied to all streams of a value of type `type`, is lossy for any of them,
/// e.g. `SZ3` for `Array(Float64)`: it rounds the floats of the values stream.
/// A codec is instantiated per stream, so the question is asked for each value stream of `type` with the type of
/// that stream; the structural streams are skipped, because they get only the generic part of the codec.
bool isLossyCodec(const ASTPtr & codec, const DataTypePtr & type)
{
    bool is_lossy = false;
    ISerialization::StreamCallback callback = [&](const auto & substream_path)
    {
        if (!is_lossy && ISerialization::isSpecialCompressionAllowed(substream_path))
            is_lossy = CompressionCodecFactory::instance().get(codec, substream_path.back().data.type.get())->isLossyCompression();
    };
    type->getDefaultSerialization()->enumerateStreams(callback, type);
    return is_lossy;
}

/// Returns the path without the `Map` buckets, which the subcolumn codec declarations don't use.
/// For example, the path of the stream `attrs.1.values` becomes the path of `attrs.values`.
ISerialization::SubstreamPath getSubstreamPathWithoutBuckets(const ISerialization::SubstreamPath & substream_path)
{
    ISerialization::SubstreamPath path_without_buckets;
    for (const auto & elem : substream_path)
        if (elem.type != ISerialization::Substream::Bucket
            && elem.type != ISerialization::Substream::MapBucketsInfo
            && elem.type != ISerialization::Substream::MapBucketIndexes)
            path_without_buckets.push_back(elem);
    return path_without_buckets;
}

}

void validateColumnCodecs(ColumnDescription & column, const CodecValidationSettings & validation_settings)
{
    const auto & factory = CompressionCodecFactory::instance();

    /// A column without streams has nothing to compress.
    checkAliasColumnHasNoCodecs(column);

    checkNoColumnCodecWithSubcolumnCodecs(column);

    if (column.codec)
    {
        /// A column with a column codec can't have subcolumn codecs, `checkNoColumnCodecWithSubcolumnCodecs` has already checked that.
        chassert(column.subcolumn_codecs.empty());
        try
        {
            column.codec = factory.validateCodecAndGetPreprocessedAST(column.codec, column.type, validation_settings);
        }
        catch (Exception & exception)
        {
            exception.addMessage("while validating the codec of column {}", backQuote(column.name));
            throw;
        }
        return;
    }

    for (auto & [subcolumn_name, subcolumn_codec] : column.subcolumn_codecs)
    {
        /// The declared subcolumn must exist in the current type of the column; this is checked again after a type change.
        auto subcolumn = column.type->tryGetSubcolumnInfo(subcolumn_name);
        if (!subcolumn)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Column {} of type {} has no subcolumn {}, so a codec cannot be declared for it",
                backQuote(column.name), column.type->getName(), backQuote(subcolumn_name));

        /// Every stream of the column is covered by at most one declaration.
        checkSubcolumnCodecsDoNotNest(column, subcolumn_name, subcolumn->substreams_path);

        /// Streams below a dynamic type are not known from the schema.
        if (isInsideDynamicType(subcolumn->substreams_path))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot declare codec for subcolumn {} of column {}: it is inside a JSON, Dynamic or Variant value",
                backQuote(subcolumn_name), backQuote(column.name));

        try
        {
            /// A declarative codec like `Quantized` works through a serialization attached to the column, which a subcolumn doesn't get.
            factory.checkCodecChainIsNotDeclarative(subcolumn_codec);

            /// A subcolumn codec is validated against the type of the subcolumn, like a column codec against the type of the column.
            subcolumn_codec = factory.validateCodecAndGetPreprocessedAST(subcolumn_codec, subcolumn->data.type, validation_settings);
        }
        catch (Exception & exception)
        {
            exception.addMessage(
                "while validating the codec of subcolumn {} of column {}", backQuote(subcolumn_name), backQuote(column.name));
            throw;
        }

        /// The factory rejects a lossy codec for a subcolumn that is or contains a Map. The keys of a Map the subcolumn
        /// is inside of are not visible in its type, so they are checked here; the values of a Map may be lossy.
        if (isInsideMapKeys(subcolumn->substreams_path) && isLossyCodec(subcolumn_codec, subcolumn->data.type))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Codec declared for subcolumn {} of column {} is lossy and cannot be applied to the keys of a Map, "
                "which would be corrupted by lossy compression",
                backQuote(subcolumn_name), backQuote(column.name));
    }
}

void validateColumnCodecs(ColumnsDescription & columns, const CodecValidationSettings & validation_settings)
{
    /// The names are collected first, because `modify` changes the container.
    Names columns_with_codecs;
    for (const auto & column : columns)
    {
        if (column.codec || !column.subcolumn_codecs.empty())
            columns_with_codecs.push_back(column.name);

        /// Physical columns have precedence when the target of a codec declaration is resolved, so a codec cannot be
        /// declared for a subcolumn that has the same name as a column.
        for (const auto & [subcolumn_name, _] : column.subcolumn_codecs)
            if (columns.has(column.name + "." + subcolumn_name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Codec declared for subcolumn {} of column {} would be taken over by column {}, which has the same name",
                    backQuote(subcolumn_name), backQuote(column.name), backQuote(column.name + "." + subcolumn_name));
    }

    for (const auto & column_name : columns_with_codecs)
        columns.modify(column_name, [&](ColumnDescription & column) { validateColumnCodecs(column, validation_settings); });
}

ASTPtr getSubstreamCodecDescription(const ISerialization::SubstreamPath & substream_path, const ColumnDescription & column)
{
    if (column.subcolumn_codecs.empty())
        return column.codec;

    /// The declared subcolumn the stream belongs to is a prefix of its path. The prefixes of the path are tried,
    /// not the prefixes of the name: an element named `a.b` is not inside an element named `a`.
    /// Declarations of one column never nest, so the first prefix found is the only one.
    const auto path = getSubstreamPathWithoutBuckets(substream_path);
    for (size_t prefix_len = path.size(); prefix_len > 0; --prefix_len)
    {
        if (auto codec = column.subcolumn_codecs.get(ISerialization::getSubcolumnNameForStream(path, prefix_len)))
            return codec;
    }

    /// No declared subcolumn covers this stream, and a column with subcolumn codecs has no column codec,
    /// so the stream uses the default codec of the part.
    return nullptr;
}

}
