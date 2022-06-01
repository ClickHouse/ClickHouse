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
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/NestedUtils.h>
#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeAggregateFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


NativeWriter::NativeWriter(
    WriteBuffer & ostr_, UInt64 client_revision_, const Block & header_, bool remove_low_cardinality_,
    IndexForNativeFormat * index_, size_t initial_size_of_file_)
    : ostr(ostr_), client_revision(client_revision_), header(header_),
      index(index_), initial_size_of_file(initial_size_of_file_), remove_low_cardinality(remove_low_cardinality_)
{
    if (index)
    {
        ostr_concrete = typeid_cast<CompressedWriteBuffer *>(&ostr);
        if (!ostr_concrete)
            throw Exception("When need to write index for NativeWriter, ostr must be CompressedWriteBuffer.", ErrorCodes::LOGICAL_ERROR);
    }
}


void NativeWriter::flush()
{
    ostr.next();
}


static void writeData(const ISerialization & serialization, const ColumnPtr & column, WriteBuffer & ostr, UInt64 offset, UInt64 limit)
{
    /** If there are columns-constants - then we materialize them.
      * (Since the data type does not know how to serialize / deserialize constants.)
      */
    ColumnPtr full_column = column->convertToFullColumnIfConst();

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&ostr](ISerialization::SubstreamPath) -> WriteBuffer * { return &ostr; };
    settings.position_independent_encoding = false;
    settings.low_cardinality_max_dictionary_size = 0; //-V1048

    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization.serializeBinaryBulkStatePrefix(settings, state);
    serialization.serializeBinaryBulkWithMultipleStreams(*full_column, offset, limit, settings, state);
    serialization.serializeBinaryBulkStateSuffix(settings, state);
}


void NativeWriter::write(const Block & block)
{
    /// Additional information about the block.
    if (client_revision > 0)
        block.info.write(ostr);

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

        ColumnWithTypeAndName column = block.safeGetByPosition(i);

        /// Send data to old clients without low cardinality type.
        if (remove_low_cardinality || (client_revision && client_revision < DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE))
        {
            column.column = recursiveRemoveLowCardinality(column.column);
            column.type = recursiveRemoveLowCardinality(column.type);
        }

        /// Name
        writeStringBinary(column.name, ostr);

        bool include_version = client_revision >= DBMS_MIN_REVISION_WITH_AGGREGATE_FUNCTIONS_VERSIONING;
        const auto * aggregate_function_data_type = typeid_cast<const DataTypeAggregateFunction *>(column.type.get());
        if (aggregate_function_data_type && aggregate_function_data_type->isVersioned())
        {
            if (include_version)
            {
                auto version = aggregate_function_data_type->getVersionFromRevision(client_revision);
                aggregate_function_data_type->setVersion(version, /* if_empty */true);
            }
            else
            {
                aggregate_function_data_type->setVersion(0, /* if_empty */false);
            }
        }

        /// Type
        String type_name = column.type->getName();

        /// For compatibility, we will not send explicit timezone parameter in DateTime data type
        ///  to older clients, that cannot understand it.
        if (client_revision < DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE
            && startsWith(type_name, "DateTime("))
            type_name = "DateTime";

        writeStringBinary(type_name, ostr);

        /// Serialization. Dynamic, if client supports it.
        SerializationPtr serialization;
        if (client_revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION)
        {
            auto info = column.column->getSerializationInfo();
            serialization = column.type->getSerialization(*info);

            bool has_custom = info->hasCustomSerialization();
            writeBinary(static_cast<UInt8>(has_custom), ostr);
            if (has_custom)
                info->serialializeKindBinary(ostr);
        }
        else
        {
            serialization = column.type->getDefaultSerialization();
            column.column = recursiveRemoveSparse(column.column);
        }

        /// Data
        if (rows)    /// Zero items of data is always represented as zero number of bytes.
            writeData(*serialization, column.column, ostr, 0, 0);

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
}

}
