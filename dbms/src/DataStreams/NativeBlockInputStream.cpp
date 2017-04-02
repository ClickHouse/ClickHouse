#include <Core/Defines.h>

#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/CompressedReadBufferFromFile.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>

#include <DataStreams/NativeBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_INDEX;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
}

NativeBlockInputStream::NativeBlockInputStream(
    ReadBuffer & istr_, UInt64 server_revision_,
    bool use_index_,
    IndexForNativeFormat::Blocks::const_iterator index_block_it_,
    IndexForNativeFormat::Blocks::const_iterator index_block_end_)
    : istr(istr_), server_revision(server_revision_),
    use_index(use_index_), index_block_it(index_block_it_), index_block_end(index_block_end_)
{
    if (use_index)
    {
        istr_concrete = typeid_cast<CompressedReadBufferFromFile *>(&istr);
        if (!istr_concrete)
            throw Exception("When need to use index for NativeBlockInputStream, istr must be CompressedReadBufferFromFile.", ErrorCodes::LOGICAL_ERROR);

        index_column_it = index_block_it->columns.begin();
    }
}


void NativeBlockInputStream::readData(const IDataType & type, IColumn & column, ReadBuffer & istr, size_t rows)
{
    if (type.isNullable())
    {
        const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
        const IDataType & nested_type = *nullable_type.getNestedType();

        ColumnNullable & nullable_col = static_cast<ColumnNullable &>(column);
        IColumn & nested_col = *nullable_col.getNestedColumn();

        IColumn & null_map = nullable_col.getNullMapConcreteColumn();
        DataTypeUInt8{}.deserializeBinaryBulk(null_map, istr, rows, 0);

        readData(nested_type, nested_col, istr, rows);

        return;
    }
    else if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
    {
        /** For arrays, you first need to deserialize the offsets, and then the values.
        */
        IColumn & offsets_column = *typeid_cast<ColumnArray &>(column).getOffsetsColumn();
        type_arr->getOffsetsType()->deserializeBinaryBulk(offsets_column, istr, rows, 0);

        if (offsets_column.size() != rows)
            throw Exception("Cannot read all data in NativeBlockInputStream.", ErrorCodes::CANNOT_READ_ALL_DATA);

        if (rows)
            readData(
                *type_arr->getNestedType(),
                typeid_cast<ColumnArray &>(column).getData(),
                istr,
                typeid_cast<const ColumnArray &>(column).getOffsets()[rows - 1]);
    }
    else
        type.deserializeBinaryBulk(column, istr, rows, 0);    /// TODO Use avg_value_size_hint.

    if (column.size() != rows)
        throw Exception("Cannot read all data in NativeBlockInputStream.", ErrorCodes::CANNOT_READ_ALL_DATA);
}


Block NativeBlockInputStream::readImpl()
{
    Block res;

    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    if (use_index && index_block_it == index_block_end)
        return res;

    if (istr.eof())
    {
        if (use_index)
            throw Exception("Input doesn't contain all data for index.", ErrorCodes::CANNOT_READ_ALL_DATA);

        return res;
    }

    /// Additional information about the block.
    if (server_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO)
        res.info.read(istr);

    /// Dimensions
    size_t columns = 0;
    size_t rows = 0;

    if (!use_index)
    {
        readVarUInt(columns, istr);
        readVarUInt(rows, istr);
    }
    else
    {
        columns = index_block_it->num_columns;
        rows = index_block_it->num_rows;
    }

    for (size_t i = 0; i < columns; ++i)
    {
        if (use_index)
        {
            /// If the current position is what is required, the real seek does not occur.
            istr_concrete->seek(index_column_it->location.offset_in_compressed_file, index_column_it->location.offset_in_decompressed_block);
        }

        ColumnWithTypeAndName column;

        /// Name
        readBinary(column.name, istr);

        /// Type
        String type_name;
        readBinary(type_name, istr);
        column.type = data_type_factory.get(type_name);

        if (use_index)
        {
            /// Index allows to do more checks.
            if (index_column_it->name != column.name)
                throw Exception("Index points to column with wrong name: corrupted index or data", ErrorCodes::INCORRECT_INDEX);
            if (index_column_it->type != type_name)
                throw Exception("Index points to column with wrong type: corrupted index or data", ErrorCodes::INCORRECT_INDEX);
        }

        /// Data
        column.column = column.type->createColumn();

        if (rows)    /// If no rows, nothing to read.
            readData(*column.type, *column.column, istr, rows);

        res.insert(std::move(column));

        if (use_index)
            ++index_column_it;
    }

    if (use_index)
    {
        if (index_column_it != index_block_it->columns.end())
            throw Exception("Inconsistent index: not all columns were read", ErrorCodes::INCORRECT_INDEX);

        ++index_block_it;
        if (index_block_it != index_block_end)
            index_column_it = index_block_it->columns.begin();
    }

    return res;
}


void IndexForNativeFormat::read(ReadBuffer & istr, const NameSet & required_columns)
{
    while (!istr.eof())
    {
        blocks.emplace_back();
        IndexOfBlockForNativeFormat & block = blocks.back();

        readVarUInt(block.num_columns, istr);
        readVarUInt(block.num_rows, istr);

        if (block.num_columns < required_columns.size())
            throw Exception("Index contain less than required columns", ErrorCodes::INCORRECT_INDEX);

        for (size_t i = 0; i < block.num_columns; ++i)
        {
            IndexOfOneColumnForNativeFormat column_index;

            readBinary(column_index.name, istr);
            readBinary(column_index.type, istr);
            readBinary(column_index.location.offset_in_compressed_file, istr);
            readBinary(column_index.location.offset_in_decompressed_block, istr);

            if (required_columns.count(column_index.name))
                block.columns.push_back(std::move(column_index));
        }

        if (block.columns.size() < required_columns.size())
            throw Exception("Index contain less than required columns", ErrorCodes::INCORRECT_INDEX);
        if (block.columns.size() > required_columns.size())
            throw Exception("Index contain duplicate columns", ErrorCodes::INCORRECT_INDEX);

        block.num_columns = block.columns.size();
    }
}


}
