#include <Core/Defines.h>
#include <Core/ProtocolDefines.h>

#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <Compression/CompressedReadBufferFromFile.h>

#include <DataTypes/DataTypeFactory.h>
#include <Common/typeid_cast.h>
#include <base/range.h>

#include <Formats/NativeReader.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <DataTypes/DataTypeAggregateFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_INDEX;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
}


NativeReader::NativeReader(ReadBuffer & istr_, UInt64 server_revision_)
    : istr(istr_), server_revision(server_revision_)
{
}

NativeReader::NativeReader(ReadBuffer & istr_, const Block & header_, UInt64 server_revision_)
    : istr(istr_), header(header_), server_revision(server_revision_)
{
}

NativeReader::NativeReader(ReadBuffer & istr_, UInt64 server_revision_,
    IndexForNativeFormat::Blocks::const_iterator index_block_it_,
    IndexForNativeFormat::Blocks::const_iterator index_block_end_)
    : istr(istr_), server_revision(server_revision_),
    use_index(true), index_block_it(index_block_it_), index_block_end(index_block_end_)
{
    istr_concrete = typeid_cast<CompressedReadBufferFromFile *>(&istr);
    if (!istr_concrete)
        throw Exception("When need to use index for NativeReader, istr must be CompressedReadBufferFromFile.", ErrorCodes::LOGICAL_ERROR);

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

void NativeReader::resetParser()
{
    istr_concrete = nullptr;
    use_index = false;
}

void NativeReader::readData(const ISerialization & serialization, ColumnPtr & column, ReadBuffer & istr, size_t rows, double avg_value_size_hint)
{
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &istr; };
    settings.avg_value_size_hint = avg_value_size_hint;
    settings.position_independent_encoding = false;
    settings.native_format = true;

    ISerialization::DeserializeBinaryBulkStatePtr state;

    serialization.deserializeBinaryBulkStatePrefix(settings, state);
    serialization.deserializeBinaryBulkWithMultipleStreams(column, rows, settings, state, nullptr);

    if (column->size() != rows)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Cannot read all data in NativeReader. Rows read: {}. Rows expected: {}", column->size(), rows);
}


Block NativeReader::getHeader() const
{
    return header;
}


Block NativeReader::read()
{
    Block res;

    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    if (use_index && index_block_it == index_block_end)
        return res;

    if (istr.eof())
    {
        if (use_index)
            throw ParsingException("Input doesn't contain all data for index.", ErrorCodes::CANNOT_READ_ALL_DATA);

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

        const auto * aggregate_function_data_type = typeid_cast<const DataTypeAggregateFunction *>(column.type.get());
        if (aggregate_function_data_type && aggregate_function_data_type->isVersioned())
        {
            auto version = aggregate_function_data_type->getVersionFromRevision(server_revision);
            aggregate_function_data_type->setVersion(version, /*if_empty=*/ true);
        }

        SerializationPtr serialization;
        if (server_revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION)
        {
            auto info = column.type->createSerializationInfo({});

            UInt8 has_custom;
            readBinary(has_custom, istr);
            if (has_custom)
                info->deserializeFromKindsBinary(istr);

            serialization = column.type->getSerialization(*info);
        }
        else
        {
            serialization = column.type->getDefaultSerialization();
        }

        if (use_index)
        {
            /// Index allows to do more checks.
            if (index_column_it->name != column.name)
                throw Exception("Index points to column with wrong name: corrupted index or data", ErrorCodes::INCORRECT_INDEX);
            if (index_column_it->type != type_name)
                throw Exception("Index points to column with wrong type: corrupted index or data", ErrorCodes::INCORRECT_INDEX);
        }

        /// Data
        ColumnPtr read_column = column.type->createColumn(*serialization);

        double avg_value_size_hint = avg_value_size_hints.empty() ? 0 : avg_value_size_hints[i];
        if (rows)    /// If no rows, nothing to read.
            readData(*serialization, read_column, istr, rows, avg_value_size_hint);

        column.column = std::move(read_column);

        if (header)
        {
            /// Support insert from old clients without low cardinality type.
            auto & header_column = header.getByName(column.name);
            if (!header_column.type->equals(*column.type))
            {
                column.column = recursiveTypeConversion(column.column, column.type, header.safeGetByPosition(i).type);
                column.type = header.safeGetByPosition(i).type;
            }
        }

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

    if (rows && header)
    {
        /// Allow to skip columns. Fill them with default values.
        Block tmp_res;

        for (auto & col : header)
        {
            if (res.has(col.name))
                tmp_res.insert(res.getByName(col.name));
            else
                tmp_res.insert({col.type->createColumn()->cloneResized(rows), col.type, col.name});
        }

        res.swap(tmp_res);
    }

    return res;
}

void NativeReader::updateAvgValueSizeHints(const Block & block)
{
    auto rows = block.rows();
    if (rows < 10)
        return;

    avg_value_size_hints.resize_fill(block.columns(), 0);

    for (auto idx : collections::range(0, block.columns()))
    {
        auto & avg_value_size_hint = avg_value_size_hints[idx];
        IDataType::updateAvgValueSizeHint(*block.getByPosition(idx).column, avg_value_size_hint);
    }
}

}
