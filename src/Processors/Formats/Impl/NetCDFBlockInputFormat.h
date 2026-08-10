#pragma once

#include <Core/NamesAndTypes.h>
#include <Formats/FormatSettings.h>
#include <Formats/NetCDF.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

#include <memory>
#include <optional>
#include <vector>

namespace DB
{

class SeekableReadBuffer;

/// How the variables of a NetCDF file are mapped to the columns and the rows of a table.
///
/// Every variable becomes a column, and the rows enumerate the Cartesian product of all the
/// dimensions that the variables use. A variable that does not use some of these dimensions has the
/// same value repeated for every index along them. This is the same table that the `to_dataframe`
/// method of `xarray` produces.
struct NetCDFTableLayout
{
    struct Column
    {
        String name;
        DataTypePtr type;

        /// The variable the column reads, or nullptr when the column holds the index along a
        /// dimension of the row space.
        const NetCDFVariable * variable = nullptr;
        /// The position of the dimension in the row space, for a column holding a dimension index.
        size_t dimension_position = 0;

        /// A `char` variable is read as a String whose length is the last dimension of the variable.
        bool is_string = false;
        /// Whether the last dimension of the variable was taken as the length of the strings. Only
        /// such a variable pads a shorter string with zero bytes, so only its values are trimmed.
        /// A `char` variable whose dimensions all stay in the row space keeps its bytes as they are.
        bool has_string_length_dimension = false;
        /// The number of bytes of one value.
        UInt64 element_size = 1;
        /// The number of values in the variable.
        UInt64 num_elements = 0;
        /// The dimensions of the variable, without the one that holds the length of the strings.
        std::vector<size_t> dimension_ids;
        /// The value that is read as NULL, in the representation of the file. Empty for a column
        /// that is not Nullable.
        String null_value;
    };

    /// The dimensions of the file, in the order in which they enumerate the rows.
    std::vector<size_t> row_dimensions;
    std::vector<UInt64> row_dimension_lengths;
    UInt64 num_rows = 0;
    std::vector<Column> columns;

    NamesAndTypesList getNamesAndTypes() const;
};

NetCDFTableLayout getNetCDFTableLayout(const NetCDFHeader & header, const FormatSettings & settings);

class NetCDFBlockInputFormat final : public IInputFormat
{
public:
    NetCDFBlockInputFormat(ReadBuffer & in_, SharedHeader header_, const FormatSettings & format_settings_);

    String getName() const override { return "NetCDFBlockInputFormat"; }

    void resetParser() override;

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

protected:
    Chunk read() override;

private:
    /// A column of the result together with the state of reading the variable behind it.
    struct ColumnState
    {
        const NetCDFTableLayout::Column * column = nullptr;

        /// How much the index of the value changes when the index along a row dimension grows by one.
        std::vector<UInt64> strides;
        /// True when the index of the value is the number of the row, which is the common case of a
        /// variable that uses all the dimensions of the row space.
        bool is_identity = false;
        /// True when the variable has a single value that is repeated in every row.
        bool is_constant = false;

        /// The part of the data of the variable that is currently in memory.
        String buffer;
        UInt64 buffer_first_element = 0;
        UInt64 buffer_num_elements = 0;
    };

    void initialize();
    /// Reads a range of the values of the variable of the column into `to`.
    void readElements(const NetCDFTableLayout::Column & column, UInt64 first_element, UInt64 num_elements_to_read, char * to);
    /// Reads the data of the variable at the given range of values into the buffer of the column.
    void loadElements(ColumnState & state, UInt64 first_element, UInt64 num_elements_to_read);
    /// Reads only the values at the given indexes into the buffer of the column and replaces the
    /// indexes with the positions of the values in the buffer. For a variable whose order of the
    /// dimensions disagrees with the order of the rows, where the values of one chunk are spread
    /// over a range that is arbitrarily larger than the chunk.
    void loadElementsSparse(ColumnState & state, PaddedPODArray<UInt64> & indexes);
    /// Reads `size` bytes at the given offset from the beginning of the file.
    void readAt(UInt64 offset, UInt64 size, char * to);
    /// The indexes of the values of the variable for a range of rows.
    void getElementIndexes(const ColumnState & state, UInt64 first_row, UInt64 count, PaddedPODArray<UInt64> & indexes) const;
    void fillColumn(const ColumnState & state, IColumn & column, const PaddedPODArray<UInt64> & indexes) const;

    const FormatSettings format_settings;

    bool is_initialized = false;
    NetCDFHeader netcdf_header;
    NetCDFTableLayout layout;
    std::vector<ColumnState> states;

    /// The whole file, when the input is not seekable and has to be read into memory.
    String content;
    SeekableReadBuffer * seekable_in = nullptr;
    UInt64 file_size = 0;

    UInt64 current_row = 0;
    size_t approx_bytes_read_for_chunk = 0;
};

class NetCDFSchemaReader final : public ISchemaReader
{
public:
    NetCDFSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;
    std::optional<size_t> readNumberOrRows() override;

private:
    void initialize();

    const FormatSettings format_settings;
    bool is_initialized = false;
    /// The size of the file, when the input is seekable. Without it the header cannot be checked
    /// against the data of the file, and the number of rows it declares is not published.
    std::optional<UInt64> file_size;
    NetCDFHeader netcdf_header;
    NetCDFTableLayout layout;
};

}
