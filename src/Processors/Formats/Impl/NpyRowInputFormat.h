#pragma once

#include <vector>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Formats/NumpyDataTypes.h>

namespace DB
{

class ReadBuffer;

struct NumpyHeader
{
    std::vector<int> shape;
    std::shared_ptr<NumpyDataType> numpy_type;
};

class NpyRowInputFormat final : public IRowInputFormat
{
public:
    NpyRowInputFormat(ReadBuffer & in_, Block header_, Params params_);

    String getName() const override { return "NpyRowInputFormat"; }

private:
    bool supportsCountRows() const override { return true; }
    size_t countRows(size_t max_block_size) override;

    void readPrefix() override;
    bool readRow(MutableColumns & columns, RowReadExtension &) override;
    void readData(MutableColumns & columns);

    template <typename T>
    void readAndInsertInteger(IColumn * column, const DataTypePtr & data_type, const NumpyDataType & npy_type);

    template <typename T>
    void readAndInsertFloat(IColumn * column, const DataTypePtr & data_type, const NumpyDataType & npy_type);

    template <typename T>
    void readAndInsertString(MutableColumnPtr column, const DataTypePtr & data_type, const NumpyDataType & npy_type, bool is_fixed);

    template <typename ColumnValue, typename DataValue>
    void readBinaryValueAndInsert(MutableColumnPtr column, NumpyDataType::Endianness endianness);

    template <typename ColumnValue>
    void readBinaryValueAndInsertFloat16(MutableColumnPtr column, NumpyDataType::Endianness endianness);

    void readRows(MutableColumns & columns);

    void readValue(IColumn * column);

    DataTypePtr nested_type;
    NumpyHeader header;
    size_t counted_rows = 0;
};

class NpySchemaReader : public ISchemaReader
{
public:
    explicit NpySchemaReader(ReadBuffer & in_);

private:
    std::optional<size_t> readNumberOrRows() override;
    NamesAndTypesList readSchema() override;
    NumpyHeader header;
};

}
