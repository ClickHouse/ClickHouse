#include <Core/Defines.h>
#include <Core/Block.h>

#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>
#include <IO/CompressedWriteBuffer.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

#include <DataStreams/MarkInCompressedFile.h>
#include <DataStreams/NativeBlockOutputStream.h>


namespace DB
{

NativeBlockOutputStream::NativeBlockOutputStream(
    WriteBuffer & ostr_, UInt64 client_revision_,
    WriteBuffer * index_ostr_, size_t initial_size_of_file_)
    : ostr(ostr_), client_revision(client_revision_),
    index_ostr(index_ostr_), initial_size_of_file(initial_size_of_file_)
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


void NativeBlockOutputStream::writeData(const IDataType & type, const ColumnPtr & column, WriteBuffer & ostr, size_t offset, size_t limit)
{
    /** If there are columns-constants - then we materialize them.
      * (Since the data type does not know how to serialize / deserialize constants.)
      */
    ColumnPtr full_column;

    if (auto converted = column->convertToFullColumnIfConst())
        full_column = converted;
    else
        full_column = column;

    if (type.isNullable())
    {
        const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
        const IDataType & nested_type = *nullable_type.getNestedType();

        const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*full_column.get());
        const ColumnPtr & nested_col = nullable_col.getNestedColumn();

        const IColumn & null_map = nullable_col.getNullMapConcreteColumn();
        DataTypeUInt8{}.serializeBinaryBulk(null_map, ostr, offset, limit);

        writeData(nested_type, nested_col, ostr, offset, limit);
    }
    else if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
    {
        /** For arrays, you first need to serialize the offsets, and then the values.
          */
        const ColumnArray & column_array = typeid_cast<const ColumnArray &>(*full_column);
        type_arr->getOffsetsType()->serializeBinaryBulk(*column_array.getOffsetsColumn(), ostr, offset, limit);

        if (!typeid_cast<const ColumnArray &>(*full_column).getData().empty())
        {
            const ColumnArray::Offsets_t & offsets = column_array.getOffsets();

            if (offset > offsets.size())
                return;

            /** offset - from which array to write.
              * limit - how many arrays should be written, or 0, if you write everything that is.
              * end - up to which array written part finishes.
              *
              * nested_offset - from which nested element to write.
              * nested_limit - how many nested elements to write, or 0, if you write everything that is.
              */

            size_t end = std::min(offset + limit, offsets.size());

            size_t nested_offset = offset ? offsets[offset - 1] : 0;
            size_t nested_limit = limit
                ? offsets[end - 1] - nested_offset
                : 0;

            const DataTypePtr & nested_type = type_arr->getNestedType();

            DataTypePtr actual_type;
            if (nested_type->isNull())
            {
                /// Special case: an array of Null is actually an array of Nullable(UInt8).
                actual_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
            }
            else
                actual_type = nested_type;

            if (limit == 0 || nested_limit)
                writeData(*actual_type, typeid_cast<const ColumnArray &>(*full_column).getDataPtr(), ostr, nested_offset, nested_limit);
        }
    }
    else
        type.serializeBinaryBulk(*full_column, ostr, offset, limit);
}


void NativeBlockOutputStream::write(const Block & block)
{
    /// Additional information about the block.
    if (client_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO)
        block.info.write(ostr);

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

        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);

        /// Name
        writeStringBinary(column.name, ostr);

        /// Type
        writeStringBinary(column.type->getName(), ostr);

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
