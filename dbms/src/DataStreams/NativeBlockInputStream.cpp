#include <Core/Defines.h>

#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/CompressedReadBufferFromFile.h>

#include <DataTypes/DataTypeFactory.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>

#include <DataStreams/NativeBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_INDEX;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NOT_IMPLEMENTED;
}


NativeBlockInputStream::NativeBlockInputStream(ReadBuffer & istr_, UInt64 server_revision_)
    : istr(istr_), server_revision(server_revision_)
{
}

NativeBlockInputStream::NativeBlockInputStream(ReadBuffer & istr_, const Block & header_, UInt64 server_revision_)
    : istr(istr_), header(header_), server_revision(server_revision_)
{
}

NativeBlockInputStream::NativeBlockInputStream(ReadBuffer & istr_, UInt64 server_revision_,
    IndexForNativeFormat::Blocks::const_iterator index_block_it_,
    IndexForNativeFormat::Blocks::const_iterator index_block_end_)
    : istr(istr_), server_revision(server_revision_),
    use_index(true), index_block_it(index_block_it_), index_block_end(index_block_end_)
{
    istr_concrete = typeid_cast<CompressedReadBufferFromFile *>(&istr);
    if (!istr_concrete)
        throw Exception("When need to use index for NativeBlockInputStream, istr must be CompressedReadBufferFromFile.", ErrorCodes::LOGICAL_ERROR);

    if (index_block_it == index_block_end)
        return;

    index_column_it = index_block_it->columns.begin();

    /// Initialize header from the index.
    for (const auto & column : index_block_it->columns)
    {
        auto type = DataTypeFactory::instance().get(column.type);
        header.insert(ColumnWithTypeAndName{ type, column.name });
    }
}


void NativeBlockInputStream::readData(const IDataType & type, IColumn & column, ReadBuffer & istr, size_t rows, double avg_value_size_hint)
{
    IDataType::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](IDataType::SubstreamPath) -> ReadBuffer * { return &istr; };
    settings.avg_value_size_hint = avg_value_size_hint;
    settings.position_independent_encoding = false;

    IDataType::DeserializeBinaryBulkStatePtr state;
    type.deserializeBinaryBulkStatePrefix(settings, state);
    type.deserializeBinaryBulkWithMultipleStreams(column, rows, settings, state);

    if (column.size() != rows)
        throw Exception("Cannot read all data in NativeBlockInputStream. Rows read: " + toString(column.size()) + ". Rows expected: " + toString(rows) + ".",
            ErrorCodes::CANNOT_READ_ALL_DATA);
}


Block NativeBlockInputStream::getHeader() const
{
    return header;
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
    if (server_revision > 0)
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
        MutableColumnPtr read_column = column.type->createColumn();

        double avg_value_size_hint = avg_value_size_hints.empty() ? 0 : avg_value_size_hints[i];
        if (rows)    /// If no rows, nothing to read.
            readData(*column.type, *read_column, istr, rows, avg_value_size_hint);

        column.column = std::move(read_column);

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

void NativeBlockInputStream::updateAvgValueSizeHints(const Block & block)
{
    auto rows = block.rows();
    if (rows < 10)
        return;

    avg_value_size_hints.resize_fill(block.columns(), 0);

    for (auto idx : ext::range(0, block.columns()))
    {
        auto & avg_value_size_hint = avg_value_size_hints[idx];
        IDataType::updateAvgValueSizeHint(*block.getByPosition(idx).column, avg_value_size_hint);
    }
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
