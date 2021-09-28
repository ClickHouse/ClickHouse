#include <Core/Defines.h>
#include <Core/Block.h>

#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>
#include <Compression/CompressedWriteBuffer.h>

#include <DataStreams/MarkInCompressedFile.h>
#include <DataStreams/NativeBlockOutputStream.h>

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


NativeBlockOutputStream::NativeBlockOutputStream(
    WriteBuffer & ostr_, UInt64 client_revision_, const Block & header_, bool remove_low_cardinality_,
    WriteBuffer * index_ostr_, size_t initial_size_of_file_)
    : ostr(ostr_), client_revision(client_revision_), header(header_),
    index_ostr(index_ostr_), initial_size_of_file(initial_size_of_file_), remove_low_cardinality(remove_low_cardinality_)
{
    if (index_ostr)
    {
        ostr_concrete = typeid_cast<CompressedWriteBuffer *>(&ostr);
        if (!ostr_concrete)
            throw Exception("When need to write index for NativeBlockOutputStream, ostr must be CompressedWriteBuffer.", ErrorCodes::LOGICAL_ERROR);
    }
}


void NativeBlockOutputStream::flush()
{
    ostr.next();
}


static void writeData(const IDataType & type, const ColumnPtr & column, WriteBuffer & ostr, UInt64 offset, UInt64 limit)
{
    /** If there are columns-constants - then we materialize them.
      * (Since the data type does not know how to serialize / deserialize constants.)
      */
    ColumnPtr full_column = column->convertToFullColumnIfConst();

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&ostr](ISerialization::SubstreamPath) -> WriteBuffer * { return &ostr; };
    settings.position_independent_encoding = false;
    settings.low_cardinality_max_dictionary_size = 0; //-V1048

    auto serialization = type.getDefaultSerialization();

    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization->serializeBinaryBulkStatePrefix(settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(*full_column, offset, limit, settings, state);
    serialization->serializeBinaryBulkStateSuffix(settings, state);
}


void NativeBlockOutputStream::write(const Block & block)
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
    if (index_ostr)
    {
        writeVarUInt(columns, *index_ostr);
        writeVarUInt(rows, *index_ostr);
    }

    for (size_t i = 0; i < columns; ++i)
    {
        /// For the index.
        MarkInCompressedFile mark;

        if (index_ostr)
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

        /// Type
        String type_name = column.type->getName();

        /// For compatibility, we will not send explicit timezone parameter in DateTime data type
        ///  to older clients, that cannot understand it.
        if (client_revision < DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE
            && startsWith(type_name, "DateTime("))
            type_name = "DateTime";

        writeStringBinary(type_name, ostr);

        /// Data
        if (rows)    /// Zero items of data is always represented as zero number of bytes.
            writeData(*column.type, column.column, ostr, 0, 0);

        if (index_ostr)
        {
            writeStringBinary(column.name, *index_ostr);
            writeStringBinary(column.type->getName(), *index_ostr);

            writeBinary(mark.offset_in_compressed_file, *index_ostr);
            writeBinary(mark.offset_in_decompressed_block, *index_ostr);
        }
    }
}

}
