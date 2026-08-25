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
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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

        /// Type
        if (format_settings && format_settings->native.encode_types_in_binary_format)
        {
            encodeDataType(column.type, ostr);
        }
        else
        {
            String type_name = column.type->getName();

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
            index_block.columns[i].type = column.type->getName();
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
