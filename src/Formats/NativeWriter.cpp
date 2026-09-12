#include <Core/ProtocolDefines.h>
#include <Core/Block.h>

#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>
#include <Compression/CompressedWriteBuffer.h>
#include <DataTypes/Serializations/SerializationInfo.h>

#include <Formats/IndexForNativeFormat.h>
#include <Formats/MarkInCompressedFile.h>
#include <Formats/NativeWriter.h>

#include <Common/typeid_cast.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnReplicated.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/FieldVisitorToString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Renders a type name taking syntax from `cached` and aggregate-function versions from `live`. Only
/// `cached` remembers spellings such as `Nested(...)`; only `live` carries the negotiated versions.
/// Returns nullopt when no version below `cached` needs correcting, leaving the caller on `getName()`.
/// `inside_simple_aggregate_function`: a version-0 leaf must be printed explicitly there, since the alias
/// rebuilds it by parsing the name back. `emit_version_token`: false for a peer with no version grammar.
std::optional<String> renderTypeNameWithLiveVersions(
    const DataTypePtr & cached, const DataTypePtr & live, bool inside_simple_aggregate_function, bool emit_version_token);

/// Picks a child's rendered name, or its plain one when that child needed no correction.
/// `std::optional::value_or(fallback->getName())` would build the fallback name unconditionally, and
/// `getName()` on a nested type is a recursive string build, so the choice is made lazily here.
String renderedOrName(std::optional<String> && rendered, const DataTypePtr & fallback)
{
    if (rendered)
        return std::move(*rendered);
    return fallback->getName();
}

/// Renders the argument as a `SimpleAggregateFunction(...)`/`Nested(...)`/container name, or falls back
/// to `cached->getName()` when this subtree needs no correction.
String renderOrGetName(const DataTypePtr & cached, const DataTypePtr & live, bool emit_version_token)
{
    if (auto rendered = renderTypeNameWithLiveVersions(
            cached, live, /*inside_simple_aggregate_function=*/false, emit_version_token))
        return *rendered;
    return cached->getName();
}

std::optional<String> renderTypeNameWithLiveVersions(
    const DataTypePtr & cached, const DataTypePtr & live, bool inside_simple_aggregate_function, bool emit_version_token)
{
    if (!cached || !live)
        return {};

    /// Customizations are matched BEFORE the plain types they are installed on: at the top of a
    /// `SimpleAggregateFunction` the cached and live pointers are the same object, so a version
    /// comparison there is trivially equal and only the customization's own cached vector diverges.
    if (const auto * simple_agg = typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(cached->getCustomName()))
    {
        const auto & arguments = simple_agg->getArgumentsDataTypes();
        /// The customization's cached vector and the storage type are the same shape by construction
        /// (`create()` derives the storage type from `argument_types[0]`), so pair element 0 with `live`.
        /// A multi-argument form cannot pair 1:1 with the storage type, so fall back to `getName()`.
        if (arguments.size() != 1)
            return {};

        auto rendered_argument = renderTypeNameWithLiveVersions(
            arguments[0], live, /*inside_simple_aggregate_function=*/true, emit_version_token);
        if (!rendered_argument)
            return {};

        WriteBufferFromOwnString stream;
        stream << "SimpleAggregateFunction(" << simple_agg->getFunctionName();
        const auto & parameters = simple_agg->getParameters();
        if (!parameters.empty())
        {
            stream << "(";
            for (size_t i = 0; i < parameters.size(); ++i)
            {
                if (i)
                    stream << ", ";
                stream << applyVisitor(FieldVisitorToString(), parameters[i]);
            }
            stream << ")";
        }
        stream << ", " << *rendered_argument << ")";
        return stream.str();
    }

    /// `Nested(...)` keeps its own spelling in a customization; recurse into the elements so it survives.
    if (const auto * nested = typeid_cast<const DataTypeNestedCustomName *>(cached->getCustomName()))
    {
        const auto * live_array = typeid_cast<const DataTypeArray *>(live.get());
        const auto * live_tuple = live_array ? typeid_cast<const DataTypeTuple *>(live_array->getNestedType().get()) : nullptr;
        const auto & elements = nested->getElements();
        if (!live_tuple || live_tuple->getElements().size() != elements.size())
            return {};

        /// Keep the per-element results optional until a sibling is known to have changed: materializing
        /// the fallback `getName()` eagerly would pay a recursive string build for every element of every
        /// unchanged type, since `value_or`'s argument is evaluated even when the optional is engaged.
        std::vector<std::optional<String>> rendered(elements.size());
        bool changed = false;
        for (size_t i = 0; i < elements.size(); ++i)
        {
            rendered[i] = renderTypeNameWithLiveVersions(
                elements[i], live_tuple->getElements()[i], inside_simple_aggregate_function, emit_version_token);
            changed |= rendered[i].has_value();
        }
        if (!changed)
            return {};

        WriteBufferFromOwnString stream;
        stream << "Nested(";
        for (size_t i = 0; i < elements.size(); ++i)
        {
            if (i)
                stream << ", ";
            stream << backQuoteIfNeed(nested->getNames()[i]) << ' ' << renderedOrName(std::move(rendered[i]), elements[i]);
        }
        stream << ")";
        return stream.str();
    }

    /// Any other customization (geometry names, ...) cannot contain a versioned leaf that the transport
    /// assigns to, so leaving it to `getName()` is both correct and byte-identical.
    if (cached->getCustomName())
        return {};

    /// A versioned `AggregateFunction` leaf: the only place a version is emitted. Re-render exactly when
    /// the version a reader would derive from the cached spelling differs from the live one.
    if (const auto * cached_agg = typeid_cast<const DataTypeAggregateFunction *>(cached.get()))
    {
        const auto * live_agg = typeid_cast<const DataTypeAggregateFunction *>(live.get());
        if (!live_agg || !cached_agg->isVersioned())
            return {};

        const size_t live_version = live_agg->getVersion();
        size_t advertised_version = cached_agg->getVersion();
        /// Under the alias a reader re-derives this leaf by parsing the printed name, where an omitted
        /// version reads as the default. Outside one it derives the version from the revision instead.
        if (inside_simple_aggregate_function && advertised_version == 0)
            advertised_version = cached_agg->getFunction()->getDefaultVersion();
        if (advertised_version == live_version)
            return {};

        static const String prefix = "AggregateFunction(";
        /// Not `const`: the early return below moves it out, and `performance-no-automatic-move`
        /// (clang-tidy) rejects returning a `const` local because constness prevents the move.
        String without_version = cached_agg->getNameWithoutVersion();

        /// Such a peer's type parser rejects a leading literal, so the leaf keeps the versionless
        /// spelling. Still rendered rather than nullopt: a sibling may need the syntax half.
        if (!emit_version_token)
            return without_version;

        /// Splice the live version into the cached spelling: the cached argument types are kept verbatim
        /// because the version walker treats an `AggregateFunction` as a leaf and never descends into them.
        if (!without_version.starts_with(prefix))
            return {};

        WriteBufferFromOwnString stream;
        stream << prefix << live_version << ", " << std::string_view(without_version).substr(prefix.size());
        return stream.str();
    }

    /// Descend the containers `setVersionToAggregateFunctions` itself descends, and only those.
    /// `Variant` is deliberately excluded: its walker treats it as an opaque leaf, so a live leaf below a
    /// `Variant` never receives a transport-assigned version and rendering it would invent a value.
    if (const auto * cached_nullable = typeid_cast<const DataTypeNullable *>(cached.get()))
    {
        const auto * live_nullable = typeid_cast<const DataTypeNullable *>(live.get());
        if (!live_nullable)
            return {};
        if (auto nested = renderTypeNameWithLiveVersions(
                cached_nullable->getNestedType(), live_nullable->getNestedType(), inside_simple_aggregate_function, emit_version_token))
            return "Nullable(" + *nested + ")";
        return {};
    }

    if (const auto * cached_array = typeid_cast<const DataTypeArray *>(cached.get()))
    {
        const auto * live_array = typeid_cast<const DataTypeArray *>(live.get());
        if (!live_array)
            return {};
        if (auto nested = renderTypeNameWithLiveVersions(
                cached_array->getNestedType(), live_array->getNestedType(), inside_simple_aggregate_function, emit_version_token))
            return "Array(" + *nested + ")";
        return {};
    }

    if (const auto * cached_tuple = typeid_cast<const DataTypeTuple *>(cached.get()))
    {
        const auto * live_tuple = typeid_cast<const DataTypeTuple *>(live.get());
        const auto & elements = cached_tuple->getElements();
        if (!live_tuple || live_tuple->getElements().size() != elements.size())
            return {};

        /// Optional until a sibling changed, for the same reason as in the `Nested` branch above.
        std::vector<std::optional<String>> rendered(elements.size());
        bool changed = false;
        for (size_t i = 0; i < elements.size(); ++i)
        {
            rendered[i] = renderTypeNameWithLiveVersions(
                elements[i], live_tuple->getElements()[i], inside_simple_aggregate_function, emit_version_token);
            changed |= rendered[i].has_value();
        }
        if (!changed)
            return {};

        WriteBufferFromOwnString stream;
        stream << "Tuple(";
        for (size_t i = 0; i < elements.size(); ++i)
        {
            if (i)
                stream << ", ";
            if (cached_tuple->hasExplicitNames())
                stream << backQuoteIfNeed(cached_tuple->getElementNames()[i]) << ' ';
            stream << renderedOrName(std::move(rendered[i]), elements[i]);
        }
        stream << ")";
        return stream.str();
    }

    if (const auto * cached_map = typeid_cast<const DataTypeMap *>(cached.get()))
    {
        const auto * live_map = typeid_cast<const DataTypeMap *>(live.get());
        if (!live_map)
            return {};
        auto key = renderTypeNameWithLiveVersions(
            cached_map->getKeyType(), live_map->getKeyType(), inside_simple_aggregate_function, emit_version_token);
        auto value = renderTypeNameWithLiveVersions(
            cached_map->getValueType(), live_map->getValueType(), inside_simple_aggregate_function, emit_version_token);
        if (!key && !value)
            return {};
        return "Map(" + renderedOrName(std::move(key), cached_map->getKeyType()) + ", "
            + renderedOrName(std::move(value), cached_map->getValueType()) + ")";
    }

    return {};
}

}


NativeWriter::NativeWriter(
    WriteBuffer & ostr_,
    UInt64 client_revision_,
    SharedHeader header_,
    std::optional<FormatSettings> format_settings_,
    bool remove_low_cardinality_,
    IndexForNativeFormat * index_,
    size_t initial_size_of_file_)
    : ostr(ostr_)
    , client_revision(client_revision_)
    , header(header_)
    , index(index_)
    , initial_size_of_file(initial_size_of_file_)
    , remove_low_cardinality(remove_low_cardinality_)
    , format_settings(std::move(format_settings_))
{
    if (index)
    {
        ostr_concrete = typeid_cast<CompressedWriteBuffer *>(&ostr);
        if (!ostr_concrete)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "When need to write index for NativeWriter, ostr must be CompressedWriteBuffer.");
    }
}


void NativeWriter::flush()
{
    ostr.next();
}

/*static*/ void NativeWriter::writeData(
    const ISerialization & serialization,
    const ColumnPtr & column,
    WriteBuffer & ostr,
    const std::optional<FormatSettings> & format_settings,
    UInt64 offset,
    UInt64 limit,
    UInt64 client_revision)
{
    /** If there are columns-constants - then we materialize them.
      * (Since the data type does not know how to serialize / deserialize constants.)
      * The same for compressed columns in-memory.
      */
    ColumnPtr full_column = column->convertToFullColumnIfConst()->decompress();

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&ostr](ISerialization::SubstreamPath) -> WriteBuffer * { return &ostr; };
    settings.position_independent_encoding = false;
    settings.low_cardinality_max_dictionary_size = 0;
    settings.native_format = true;
    settings.format_settings = format_settings ? &*format_settings : nullptr;
    if (client_revision < DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON_SERIALIZATION)
    {
        settings.dynamic_serialization_version = MergeTreeDynamicSerializationVersion::V1;
        settings.object_serialization_version = MergeTreeObjectSerializationVersion::V1;
    }
    else
    {
        settings.dynamic_serialization_version = MergeTreeDynamicSerializationVersion::V2;
        settings.object_serialization_version = MergeTreeObjectSerializationVersion::V2;
    }

    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization.serializeBinaryBulkStatePrefix(*full_column, settings, state);
    serialization.serializeBinaryBulkWithMultipleStreams(*full_column, offset, limit, settings, state);
    serialization.serializeBinaryBulkStateSuffix(settings, state);
}

std::tuple<SerializationPtr, SerializationInfoPtr, ColumnPtr> NativeWriter::getSerializationAndColumn(UInt64 client_revision, const ColumnWithTypeAndName & column)
{
    if (client_revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION)
    {
        ColumnPtr result_column = column.column;
        if (client_revision < DBMS_MIN_REVISION_WITH_REPLICATED_SERIALIZATION)
            result_column = result_column->convertToFullColumnIfReplicated();
        if (client_revision < DBMS_MIN_REVISION_WITH_SPARSE_SERIALIZATION)
            result_column = recursiveRemoveSparse(result_column);
        if (client_revision < DBMS_MIN_REVISION_WITH_NULLABLE_SPARSE_SERIALIZATION)
        {
            if (column.type->isNullable())
                result_column = recursiveRemoveSparse(result_column);
        }

        /// The size-stream String layout follows the peer revision and needs no per-column wire marker.
        auto info = column.type->getSerializationInfo(
            *result_column,
            SerializationInfoSettings::enableAllSupportedSerializations(
                client_revision >= DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION));
        return {column.type->getSerialization(*info), info, result_column};
    }

    return {column.type->getDefaultSerialization(), nullptr, recursiveRemoveSparse(column.column->convertToFullColumnIfReplicated())};
}

size_t NativeWriter::write(const Block & block)
{
    size_t written_before = ostr.count();

    /// Additional information about the block.
    if (client_revision > 0)
        block.info.write(ostr, client_revision);

    block.checkNumberOfRows();

    /// Dimensions
    size_t columns = block.columns();
    size_t rows = block.rows();

    writeVarUInt(columns, ostr);
    writeVarUInt(rows, ostr);

    /** The index has the same structure as the data stream.
      * But instead of column values, it contains a mark that points to the location in the data file where this part of the column is located.
      */
    IndexOfBlockForNativeFormat index_block;
    if (index)
    {
        index_block.num_columns = columns;
        index_block.num_rows = rows;
        index_block.columns.resize(columns);
    }

    /// Remove unreferenced data from replicated columns before serialization.
    Columns compacted_columns = block.getColumns();
    compactReplicatedColumns(compacted_columns);

    for (size_t i = 0; i < columns; ++i)
    {
        /// For the index.
        MarkInCompressedFile mark{0, 0};

        if (index)
        {
            ostr_concrete->next();  /// Finish compressed block.
            mark.offset_in_compressed_file = initial_size_of_file + ostr_concrete->getCompressedBytes();
            mark.offset_in_decompressed_block = ostr_concrete->getRemainingBytes();
        }

        auto column = block.safeGetByPosition(i);
        column.column = compacted_columns[i];

        /// Send data to old clients without low cardinality type.
        if (remove_low_cardinality || (client_revision && client_revision < DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE))
        {
            column.column = recursiveRemoveLowCardinality(column.column);
            column.type = recursiveRemoveLowCardinality(column.type);
        }

        /// Name
        writeStringBinary(column.name, ostr);

        /// The state version of a versioned aggregate function on the wire is derived from the
        /// negotiated revision, and the receiver derives it the same way. It must not be taken from a
        /// version pinned on the local type - a table attached from metadata that predates versioning
        /// has its columns pinned to version 0, and version 0 is not printed in the type name, so the
        /// receiver would see no version at all and read the payload with its own, higher version.
        /// Re-serializing loses nothing: the states are kept in memory in a version-independent form.
        ///
        /// Revision 0 means there is no peer to negotiate with: the block goes to a self-describing
        /// stream that is read back by whoever wrote it (`StripeLog` data, `Set`/`Join` backups, a
        /// `Native` format file). Here the version must be taken from the type: the reader derives
        /// nothing from a revision and trusts the type name in the stream, so a version pinned on a
        /// stored column (`pinCurrentStateVersionToAggregateFunctions`) has to survive, or the state
        /// would silently degrade to version 0 on every round trip through local persistence.
        bool include_version = client_revision >= DBMS_MIN_REVISION_WITH_AGGREGATE_FUNCTIONS_VERSIONING;
        setVersionToAggregateFunctions(column.type, /* if_empty= */ client_revision == 0, include_version ? std::optional<size_t>(client_revision) : std::nullopt);

        /// Whether the announced type string may carry an `AggregateFunction(<version>, ...)` token. The
        /// leading `client_revision &&` keeps revision `0` out of the branch: only a non-zero revision
        /// below the threshold is a peer that cannot parse the token.
        const bool emit_version_token
            = !(client_revision && client_revision < DBMS_MIN_REVISION_WITH_AGGREGATE_FUNCTIONS_VERSIONING);

        /// One rendering shared by the stream header and the index header, so the two cannot disagree.
        /// Lazy: a binary-encoded header without an index needs no textual name at all.
        std::optional<String> rendered_type_name;
        auto renderTypeName = [&]() -> const String &
        {
            if (!rendered_type_name)
                rendered_type_name = renderOrGetName(column.type, column.type, emit_version_token);
            return *rendered_type_name;
        };

        /// Type
        if (format_settings && format_settings->native.encode_types_in_binary_format)
        {
            encodeDataType(column.type, ostr);
        }
        else
        {
            String type_name = renderTypeName();

            /// For compatibility, we will not send explicit timezone parameter in DateTime data type
            ///  to older clients, that cannot understand it.
            if (client_revision < DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE
                && startsWith(type_name, "DateTime("))
                type_name = "DateTime";

            writeStringBinary(type_name, ostr);
        }

        /// Serialization. Dynamic, if client supports it.
        SerializationPtr serialization;
        {
            SerializationInfoPtr info;
            std::tie(serialization, info, column.column) = getSerializationAndColumn(client_revision, column);
            if (info)
            {
                writeBinary(static_cast<UInt8>(info->hasCustomSerialization()), ostr);
                if (info->hasCustomSerialization())
                    info->serialializeKindStackBinary(ostr);
            }
        }

        /// Data
        if (rows)    /// Zero items of data is always represented as zero number of bytes.
            writeData(*serialization, column.column, ostr, format_settings, 0, 0, client_revision);

        if (index)
        {
            index_block.columns[i].name = column.name;
            index_block.columns[i].type = renderTypeName();
            index_block.columns[i].location.offset_in_compressed_file = mark.offset_in_compressed_file;
            index_block.columns[i].location.offset_in_decompressed_block = mark.offset_in_decompressed_block;
        }
    }

    if (index)
        index->blocks.emplace_back(std::move(index_block));

    size_t written_after = ostr.count();
    size_t written_size = written_after - written_before;
    return written_size;
}
}
