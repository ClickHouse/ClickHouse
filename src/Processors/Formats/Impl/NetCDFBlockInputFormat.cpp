#include <Processors/Formats/Impl/NetCDFBlockInputFormat.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileSize.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Interpreters/castColumn.h>
#include <Common/assert_cast.h>
#include <Common/transformEndianness.h>
#include <Core/Defines.h>

#include <base/arithmeticOverflow.h>

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int THERE_IS_NO_COLUMN;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{

DataTypePtr getDataType(NetCDFType type)
{
    switch (type)
    {
        case NetCDFType::Byte: return std::make_shared<DataTypeInt8>();
        case NetCDFType::Char: return std::make_shared<DataTypeString>();
        case NetCDFType::Short: return std::make_shared<DataTypeInt16>();
        case NetCDFType::Int: return std::make_shared<DataTypeInt32>();
        case NetCDFType::Float: return std::make_shared<DataTypeFloat32>();
        case NetCDFType::Double: return std::make_shared<DataTypeFloat64>();
        case NetCDFType::UByte: return std::make_shared<DataTypeUInt8>();
        case NetCDFType::UShort: return std::make_shared<DataTypeUInt16>();
        case NetCDFType::UInt: return std::make_shared<DataTypeUInt32>();
        case NetCDFType::Int64: return std::make_shared<DataTypeInt64>();
        case NetCDFType::UInt64: return std::make_shared<DataTypeUInt64>();
    }
    return nullptr;
}

/// Puts the dimensions in an order that agrees with the order of the dimensions of every variable,
/// so that a variable that uses all of them is stored in exactly the order the rows are produced.
/// Among the dimensions that are not ordered relative to each other, the one that is declared first
/// in the file goes first.
std::vector<size_t> orderRowDimensions(size_t num_dimensions, const std::vector<std::vector<size_t>> & variable_dimensions)
{
    std::vector<bool> is_used(num_dimensions, false);
    std::vector<std::unordered_set<size_t>> successors(num_dimensions);
    std::vector<size_t> in_degree(num_dimensions, 0);
    size_t num_used = 0;

    for (const auto & dimensions : variable_dimensions)
    {
        for (size_t i = 0; i < dimensions.size(); ++i)
        {
            if (!is_used[dimensions[i]])
            {
                is_used[dimensions[i]] = true;
                ++num_used;
            }

            if (i + 1 < dimensions.size() && successors[dimensions[i]].insert(dimensions[i + 1]).second)
                ++in_degree[dimensions[i + 1]];
        }
    }

    std::vector<size_t> order;
    order.reserve(num_used);
    std::vector<bool> is_ordered(num_dimensions, false);

    while (order.size() < num_used)
    {
        size_t chosen = num_dimensions;
        for (size_t i = 0; i < num_dimensions && chosen == num_dimensions; ++i)
            if (is_used[i] && !is_ordered[i] && in_degree[i] == 0)
                chosen = i;

        /// The orders of the dimensions of two variables contradict each other, which is possible
        /// only in a file where, for example, one variable has the dimensions (x, y) and another
        /// one has (y, x). Any order will disagree with one of them, so take the first dimension
        /// that is left.
        for (size_t i = 0; i < num_dimensions && chosen == num_dimensions; ++i)
            if (is_used[i] && !is_ordered[i])
                chosen = i;

        is_ordered[chosen] = true;
        order.push_back(chosen);

        for (size_t successor : successors[chosen])
            if (in_degree[successor] != 0)
                --in_degree[successor];
    }

    return order;
}

}

NamesAndTypesList NetCDFTableLayout::getNamesAndTypes() const
{
    NamesAndTypesList result;
    for (const auto & column : columns)
        result.emplace_back(column.name, column.type);
    return result;
}

NetCDFTableLayout getNetCDFTableLayout(const NetCDFHeader & header, const FormatSettings & settings)
{
    NetCDFTableLayout layout;

    /// The classic format has no string type: a string is an array of characters, and the last
    /// dimension of a `char` variable is the length of the strings rather than a dimension of the
    /// row space. The unlimited dimension is never used this way, because it is always the first.
    std::vector<std::vector<size_t>> effective_dimensions(header.variables.size());
    std::vector<UInt64> string_lengths(header.variables.size(), 1);

    for (size_t i = 0; i < header.variables.size(); ++i)
    {
        const auto & variable = header.variables[i];
        effective_dimensions[i] = variable.dimension_ids;

        if (variable.type == NetCDFType::Char && !effective_dimensions[i].empty())
        {
            const auto & last = header.dimensions[effective_dimensions[i].back()];
            if (!last.is_unlimited)
            {
                string_lengths[i] = last.length;
                effective_dimensions[i].pop_back();
            }
        }

        std::unordered_set<size_t> distinct(effective_dimensions[i].begin(), effective_dimensions[i].end());
        if (distinct.size() != effective_dimensions[i].size())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "The variable {} of the NetCDF file uses the same dimension more than once, which cannot be "
                "represented as a table", variable.name);
    }

    layout.row_dimensions = orderRowDimensions(header.dimensions.size(), effective_dimensions);

    layout.num_rows = 1;
    for (size_t dimension_id : layout.row_dimensions)
    {
        layout.row_dimension_lengths.push_back(header.dimensions[dimension_id].length);
        if (common::mulOverflow(layout.num_rows, header.dimensions[dimension_id].length, layout.num_rows))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                "The number of rows of the NetCDF file does not fit in 64 bits");
    }

    std::unordered_map<size_t, size_t> position_of_dimension;
    for (size_t position = 0; position < layout.row_dimensions.size(); ++position)
        position_of_dimension[layout.row_dimensions[position]] = position;

    std::unordered_map<std::string_view, size_t> variable_ids;
    for (size_t i = 0; i < header.variables.size(); ++i)
        if (!variable_ids.emplace(header.variables[i].name, i).second)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The NetCDF file has more than one variable named {}", header.variables[i].name);

    /// The columns with the indexes along the dimensions come first, so that the columns with the
    /// data of the variables keep the order they have in the file.
    if (settings.netcdf.add_dimension_columns)
    {
        /// The name of a generated column has to differ from the name of a variable and from the
        /// name of another generated column, or one of the columns would be unreachable.
        std::unordered_set<String> used_column_names;
        for (const auto & variable : header.variables)
            used_column_names.insert(variable.name);

        for (size_t position = 0; position < layout.row_dimensions.size(); ++position)
        {
            size_t dimension_id = layout.row_dimensions[position];
            const auto & dimension = header.dimensions[dimension_id];

            /// A coordinate variable already provides the values along its dimension. It is a
            /// variable that has the name of the dimension and is one-dimensional over it - a
            /// variable that merely shares the name says nothing about the axis, and then the index
            /// along the dimension is only available as a column of its own.
            auto it = variable_ids.find(dimension.name);
            bool has_coordinate_variable = it != variable_ids.end()
                && effective_dimensions[it->second] == std::vector<size_t>{dimension_id};

            if (has_coordinate_variable)
                continue;

            /// The name of the dimension is taken by a variable that is not a coordinate variable.
            String column_name = dimension.name;
            for (size_t attempt = 1; used_column_names.contains(column_name); ++attempt)
                column_name = dimension.name + "_index" + (attempt == 1 ? "" : "_" + std::to_string(attempt));

            NetCDFTableLayout::Column column;
            column.name = std::move(column_name);
            used_column_names.insert(column.name);
            column.type = std::make_shared<DataTypeUInt64>();
            column.dimension_position = position;
            layout.columns.push_back(std::move(column));
        }
    }

    for (size_t i = 0; i < header.variables.size(); ++i)
    {
        const auto & variable = header.variables[i];

        NetCDFTableLayout::Column column;
        column.name = variable.name;
        column.variable = &variable;
        column.is_string = variable.type == NetCDFType::Char;
        column.element_size = column.is_string ? string_lengths[i] : netCDFTypeSize(variable.type);
        column.dimension_ids = effective_dimensions[i];
        column.type = getDataType(variable.type);

        column.num_elements = 1;
        for (size_t dimension_id : column.dimension_ids)
            column.num_elements *= header.dimensions[dimension_id].length;

        /// A variable can declare the value that marks the data that is missing. There is no such
        /// convention for strings, where the padding of a shorter string plays that role.
        if (settings.netcdf.fill_value_as_null && !column.is_string)
        {
            for (std::string_view attribute_name : {"_FillValue", "missing_value"})
            {
                const auto * attribute = variable.tryGetAttribute(attribute_name);
                if (attribute && attribute->type == variable.type && attribute->num_elements == 1
                    && attribute->data.size() == column.element_size)
                {
                    column.null_value = attribute->data;
                    column.type = makeNullable(column.type);
                    break;
                }
            }
        }

        layout.columns.push_back(std::move(column));
    }

    return layout;
}


NetCDFBlockInputFormat::NetCDFBlockInputFormat(ReadBuffer & in_, SharedHeader header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &in_), format_settings(format_settings_)
{
}

void NetCDFBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    is_initialized = false;
    states.clear();
    layout = {};
    netcdf_header = {};
    content.clear();
    seekable_in = nullptr;
    file_size = 0;
    current_row = 0;
    approx_bytes_read_for_chunk = 0;
}

void NetCDFBlockInputFormat::readAt(UInt64 offset, UInt64 size, char * to)
{
    if (offset > file_size || size > file_size - offset)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Cannot read {} bytes at the offset {} of the NetCDF file of {} bytes", size, offset, file_size);

    approx_bytes_read_for_chunk += size;

    if (seekable_in)
    {
        seekable_in->seek(static_cast<off_t>(offset), SEEK_SET);
        seekable_in->readStrict(to, size);
        return;
    }

    memcpy(to, content.data() + offset, size);
}

void NetCDFBlockInputFormat::initialize()
{
    is_initialized = true;

    auto * seekable = dynamic_cast<SeekableReadBuffer *>(in);
    if (seekable && format_settings.seekable_read && isBufferWithFileSize(*in) && seekable->checkIfActuallySeekable())
    {
        seekable_in = seekable;
        file_size = getFileSizeFromReadBuffer(*in);
        seekable_in->seek(0, SEEK_SET);
        netcdf_header = readNetCDFHeader(*in);
    }
    else
    {
        /// The data of the variables is scattered over the file and is not read in the order in
        /// which it is stored, so an input that cannot be seeked has to be kept in memory.
        {
            WriteBufferFromString content_out(content);
            copyData(*in, content_out);
        }

        file_size = content.size();
        ReadBufferFromString content_in(content);
        netcdf_header = readNetCDFHeader(content_in);
    }

    netcdf_header.resolveNumberOfRecords(file_size);

    for (const auto & variable : netcdf_header.variables)
    {
        UInt64 required_size = 0;
        bool overflow = false;

        if (!variable.is_record)
        {
            overflow = common::addOverflow(variable.begin, variable.slab_size, required_size);
        }
        else if (netcdf_header.num_records != 0)
        {
            overflow = common::mulOverflow(netcdf_header.num_records - 1, netcdf_header.record_size, required_size)
                || common::addOverflow(required_size, variable.begin, required_size)
                || common::addOverflow(required_size, variable.slab_size, required_size);
        }

        if (overflow || required_size > file_size)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The data of the variable {} does not fit in the NetCDF file: it needs {} bytes, but the file is {} bytes",
                variable.name, overflow ? "more than 2^64" : std::to_string(required_size), file_size);
    }

    layout = getNetCDFTableLayout(netcdf_header, format_settings);

    std::unordered_map<std::string_view, const NetCDFTableLayout::Column *> columns_by_name;
    for (const auto & column : layout.columns)
        columns_by_name.emplace(column.name, &column);

    std::unordered_map<size_t, size_t> position_of_dimension;
    for (size_t position = 0; position < layout.row_dimensions.size(); ++position)
        position_of_dimension[layout.row_dimensions[position]] = position;

    for (const auto & requested : getPort().getHeader())
    {
        auto it = columns_by_name.find(requested.name);
        if (it == columns_by_name.end())
            throw Exception(ErrorCodes::THERE_IS_NO_COLUMN,
                "The NetCDF file has no variable named {}", requested.name);

        ColumnState state;
        state.column = it->second;
        state.strides.assign(layout.row_dimensions.size(), 0);

        if (state.column->variable)
        {
            UInt64 stride = 1;
            for (size_t i = state.column->dimension_ids.size(); i > 0; --i)
            {
                size_t dimension_id = state.column->dimension_ids[i - 1];
                state.strides[position_of_dimension.at(dimension_id)] = stride;
                stride *= netcdf_header.dimensions[dimension_id].length;
            }

            state.is_identity = state.column->dimension_ids == layout.row_dimensions;
            state.is_constant = state.column->dimension_ids.empty();
        }
        else
        {
            /// The index along a dimension is its own position in the row space.
            state.strides[state.column->dimension_position] = 1;
        }

        states.push_back(std::move(state));
    }
}

void NetCDFBlockInputFormat::loadElements(ColumnState & state, UInt64 first_element, UInt64 num_elements_to_read)
{
    if (num_elements_to_read == 0)
        return;

    if (state.buffer_num_elements != 0 && first_element >= state.buffer_first_element
        && first_element + num_elements_to_read <= state.buffer_first_element + state.buffer_num_elements)
        return;

    const auto & column = *state.column;
    const auto & variable = *column.variable;

    if (first_element + num_elements_to_read > column.num_elements)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Reading the values [{}, {}) of the variable {} of the NetCDF file, which has {} values",
            first_element, first_element + num_elements_to_read, variable.name, column.num_elements);

    UInt64 remaining_bytes = num_elements_to_read * column.element_size;
    state.buffer.resize(remaining_bytes);
    state.buffer_first_element = first_element;
    state.buffer_num_elements = num_elements_to_read;

    UInt64 offset_in_data = first_element * column.element_size;
    char * to = state.buffer.data();

    if (!variable.is_record)
    {
        readAt(variable.begin + offset_in_data, remaining_bytes, to);
        return;
    }

    /// The records of all record variables are interleaved, so the data of one variable has to be
    /// collected from one record at a time.
    UInt64 record = offset_in_data / variable.slab_size;
    UInt64 offset_in_record = offset_in_data % variable.slab_size;

    while (remaining_bytes != 0)
    {
        UInt64 to_read = std::min(remaining_bytes, variable.slab_size - offset_in_record);
        readAt(variable.begin + record * netcdf_header.record_size + offset_in_record, to_read, to);

        to += to_read;
        remaining_bytes -= to_read;
        ++record;
        offset_in_record = 0;
    }
}

void NetCDFBlockInputFormat::getElementIndexes(
    const ColumnState & state, UInt64 first_row, UInt64 count, PaddedPODArray<UInt64> & indexes) const
{
    indexes.resize(count);

    if (state.is_identity)
    {
        for (UInt64 i = 0; i < count; ++i)
            indexes[i] = first_row + i;
        return;
    }

    if (state.is_constant)
    {
        for (UInt64 i = 0; i < count; ++i)
            indexes[i] = 0;
        return;
    }

    size_t num_dimensions = layout.row_dimensions.size();
    std::vector<UInt64> positions(num_dimensions);
    UInt64 rest = first_row;
    for (size_t i = num_dimensions; i > 0; --i)
    {
        positions[i - 1] = rest % layout.row_dimension_lengths[i - 1];
        rest /= layout.row_dimension_lengths[i - 1];
    }

    UInt64 index = 0;
    for (size_t i = 0; i < num_dimensions; ++i)
        index += positions[i] * state.strides[i];

    for (UInt64 row = 0; row < count; ++row)
    {
        indexes[row] = index;

        /// Move to the next row: increment the index along the last dimension and carry over.
        for (size_t i = num_dimensions; i > 0; --i)
        {
            ++positions[i - 1];
            index += state.strides[i - 1];

            if (positions[i - 1] < layout.row_dimension_lengths[i - 1])
                break;

            index -= state.strides[i - 1] * layout.row_dimension_lengths[i - 1];
            positions[i - 1] = 0;
        }
    }
}

namespace
{

template <typename T>
void insertNumbers(IColumn & column, const char * data, UInt64 first_element, const PaddedPODArray<UInt64> & indexes, const String & null_value)
{
    ColumnVector<T> * values = nullptr;
    NullMap * null_map = nullptr;

    if (auto * nullable = typeid_cast<ColumnNullable *>(&column))
    {
        values = &assert_cast<ColumnVector<T> &>(nullable->getNestedColumn());
        null_map = &nullable->getNullMapData();
    }
    else
    {
        values = &assert_cast<ColumnVector<T> &>(column);
    }

    auto & data_to = values->getData();
    size_t old_size = data_to.size();
    data_to.resize(old_size + indexes.size());

    if (null_map)
        null_map->resize_fill(old_size + indexes.size(), 0);

    for (size_t i = 0; i < indexes.size(); ++i)
    {
        const char * from = data + (indexes[i] - first_element) * sizeof(T);

        T value;
        memcpy(&value, from, sizeof(T));
        transformEndianness<std::endian::native, std::endian::big>(value);
        data_to[old_size + i] = value;

        if (null_map && 0 == memcmp(from, null_value.data(), sizeof(T)))
            (*null_map)[old_size + i] = 1;
    }
}

}

void NetCDFBlockInputFormat::fillColumn(const ColumnState & state, IColumn & column, const PaddedPODArray<UInt64> & indexes) const
{
    const auto & description = *state.column;

    /// The column with the index along a dimension is the index itself.
    if (!description.variable)
    {
        auto & data_to = assert_cast<ColumnUInt64 &>(column).getData();
        data_to.insert(indexes.begin(), indexes.end());
        return;
    }

    const char * data = state.buffer.data();

    if (description.is_string)
    {
        auto & string_column = assert_cast<ColumnString &>(column);
        for (UInt64 index : indexes)
        {
            const char * from = data + (index - state.buffer_first_element) * description.element_size;

            /// A string shorter than the dimension that holds it is padded with zero bytes.
            size_t length = description.element_size;
            while (length != 0 && from[length - 1] == '\0')
                --length;

            string_column.insertData(from, length);
        }
        return;
    }

    switch (description.variable->type)
    {
        case NetCDFType::Byte:
            insertNumbers<Int8>(column, data, state.buffer_first_element, indexes, description.null_value);
            return;
        case NetCDFType::Short:
            insertNumbers<Int16>(column, data, state.buffer_first_element, indexes, description.null_value);
            return;
        case NetCDFType::Int:
            insertNumbers<Int32>(column, data, state.buffer_first_element, indexes, description.null_value);
            return;
        case NetCDFType::Float:
            insertNumbers<Float32>(column, data, state.buffer_first_element, indexes, description.null_value);
            return;
        case NetCDFType::Double:
            insertNumbers<Float64>(column, data, state.buffer_first_element, indexes, description.null_value);
            return;
        case NetCDFType::UByte:
            insertNumbers<UInt8>(column, data, state.buffer_first_element, indexes, description.null_value);
            return;
        case NetCDFType::UShort:
            insertNumbers<UInt16>(column, data, state.buffer_first_element, indexes, description.null_value);
            return;
        case NetCDFType::UInt:
            insertNumbers<UInt32>(column, data, state.buffer_first_element, indexes, description.null_value);
            return;
        case NetCDFType::Int64:
            insertNumbers<Int64>(column, data, state.buffer_first_element, indexes, description.null_value);
            return;
        case NetCDFType::UInt64:
            insertNumbers<UInt64>(column, data, state.buffer_first_element, indexes, description.null_value);
            return;
        case NetCDFType::Char:
            break;
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected data type of the variable {}", description.name);
}

Chunk NetCDFBlockInputFormat::read()
{
    if (!is_initialized)
        initialize();

    if (current_row >= layout.num_rows)
        return {};

    approx_bytes_read_for_chunk = 0;

    UInt64 count = std::min<UInt64>(DEFAULT_BLOCK_SIZE, layout.num_rows - current_row);
    const auto & result_header = getPort().getHeader();

    Columns result;
    result.reserve(states.size());

    PaddedPODArray<UInt64> indexes;

    for (size_t i = 0; i < states.size(); ++i)
    {
        auto & state = states[i];
        getElementIndexes(state, current_row, count, indexes);

        if (state.column->variable)
        {
            /// The indexes grow and shrink as the indexes along the dimensions that the variable
            /// does not use are cycled through, so the whole range they cover is read at once. It
            /// is not read again while the following chunks stay inside it, which is what happens
            /// for the variables along the outer dimensions of the row space.
            UInt64 first_element = *std::min_element(indexes.begin(), indexes.end());
            UInt64 last_element = *std::max_element(indexes.begin(), indexes.end());
            loadElements(state, first_element, last_element - first_element + 1);
        }

        auto column = state.column->type->createColumn();
        column->reserve(count);
        fillColumn(state, *column, indexes);

        const auto & requested_type = result_header.getByPosition(i).type;
        if (requested_type->equals(*state.column->type))
            result.push_back(std::move(column));
        else
            result.push_back(castColumn({std::move(column), state.column->type, ""}, requested_type));
    }

    current_row += count;
    return Chunk(std::move(result), count);
}


NetCDFSchemaReader::NetCDFSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), format_settings(format_settings_)
{
}

void NetCDFSchemaReader::initialize()
{
    if (is_initialized)
        return;

    is_initialized = true;
    netcdf_header = readNetCDFHeader(in);
    layout = getNetCDFTableLayout(netcdf_header, format_settings);
}

NamesAndTypesList NetCDFSchemaReader::readSchema()
{
    initialize();
    return layout.getNamesAndTypes();
}

std::optional<size_t> NetCDFSchemaReader::readNumberOrRows()
{
    initialize();

    /// The number of records of a file written in the streaming mode is not in the header: it has
    /// to be calculated from the size of the file, which is not available here.
    if (netcdf_header.num_records_is_streaming)
        return std::nullopt;

    return layout.num_rows;
}


void registerInputFormatNetCDF(FormatFactory & factory);
void registerInputFormatNetCDF(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "NetCDF",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings &,
           bool /* is_remote_fs */,
           FormatParserSharedResourcesPtr,
           FormatFilterInfoPtr) -> InputFormatPtr
        { return std::make_shared<NetCDFBlockInputFormat>(buf, std::make_shared<const Block>(sample), settings); });

    factory.markFormatSupportsSubsetOfColumns("NetCDF");
    factory.registerFileExtension("nc", "NetCDF");

    factory.setDocumentation("NetCDF", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[NetCDF](https://www.unidata.ucar.edu/software/netcdf/) is a self-describing binary format for
multidimensional arrays, used mostly for climate, weather, oceanographic and other scientific data.

ClickHouse supports the three "classic" versions of the format: CDF-1 (the original classic format),
CDF-2 (the 64-bit offset format) and CDF-5 (the 64-bit data format). The NetCDF-4 format, which is a
different format built on top of HDF5, is not supported; convert such a file first, for example with
`nccopy -k cdf5 input.nc output.nc`.

## Data model {#data-model}

A NetCDF file is a set of named multidimensional arrays, called variables, over a set of named
dimensions. Every variable becomes a column, and the rows enumerate the Cartesian product of all the
dimensions that the variables use. A variable that does not use some of these dimensions has the
same value repeated for every index along them. This is the same table that the `to_dataframe`
method of the `xarray` library produces.

For example, a file with the dimensions `time`, `lat`, `lon` and the variables `time(time)`,
`lat(lat)`, `lon(lon)` and `temperature(time, lat, lon)` is read as a table with the columns `time`,
`lat`, `lon`, `temperature` and `time * lat * lon` rows, where the coordinate columns repeat.

The classic format has no string type. A `char` variable is read as a String column whose length is
the last dimension of the variable, so `char station_name(station, name_length)` is read as one
string per station. The trailing zero bytes that pad a shorter string are removed.

Attributes of the file and of the variables are not part of the table.

## Data types matching {#data_types-matching}

| NetCDF data type (`INSERT`) | ClickHouse data type                                    | NetCDF data type (`SELECT`) |
|-----------------------------|---------------------------------------------------------|-----------------------------|
| `byte`                      | [Int8](/sql-reference/data-types/int-uint.md)           | `byte`                      |
| `short`                     | [Int16](/sql-reference/data-types/int-uint.md)          | `short`                     |
| `int`                       | [Int32](/sql-reference/data-types/int-uint.md)          | `int`                       |
| `int64`                     | [Int64](/sql-reference/data-types/int-uint.md)          | `int64`                     |
| `ubyte`                     | [UInt8](/sql-reference/data-types/int-uint.md)          | `ubyte`                     |
| `ushort`                    | [UInt16](/sql-reference/data-types/int-uint.md)         | `ushort`                    |
| `uint`                      | [UInt32](/sql-reference/data-types/int-uint.md)         | `uint`                      |
| `uint64`                    | [UInt64](/sql-reference/data-types/int-uint.md)         | `uint64`                    |
| `float`                     | [Float32](/sql-reference/data-types/float.md)           | `float`                     |
| `double`                    | [Float64](/sql-reference/data-types/float.md)           | `double`                    |
| `char`                      | [String](/sql-reference/data-types/string.md)           | `char`                      |
|                             | [FixedString](/sql-reference/data-types/fixedstring.md) | `char`                      |

The types `ubyte`, `ushort`, `uint`, `int64` and `uint64` exist only in CDF-5.

## Example usage {#example-usage}

```sql title="Query"
SELECT * FROM file('temperature.nc') ORDER BY time, lat, lon LIMIT 3
```

```response title="Response"
┌─time─┬─lat─┬──lon─┬─temperature─┐
│    0 │ -90 │ -180 │      241.75 │
│    0 │ -90 │ -179 │      241.81 │
│    0 │ -90 │ -178 │      241.87 │
└──────┴─────┴──────┴─────────────┘
```

Writing a table to a file:

```sql title="Query"
SELECT * FROM measurements INTO OUTFILE 'measurements.nc' FORMAT NetCDF
```

On output every column becomes a one-dimensional variable over a single dimension named `row`, so a
file written by ClickHouse is read back with the same structure.

## Format settings {#format-settings}

| Setting                                                                                                                     | Description                                                                                     | Default |
|-----------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|---------|
| [`input_format_netcdf_fill_value_as_null`](/operations/settings/settings-formats#input_format_netcdf_fill_value_as_null)     | Read the values equal to the `_FillValue` or `missing_value` attribute of a variable as `NULL`. | `false` |
| [`input_format_netcdf_add_dimension_columns`](/operations/settings/settings-formats#input_format_netcdf_add_dimension_columns) | Add a column with the index along every dimension that has no variable of the same name.        | `false` |
)DOCS_MD",
        .introduced_in = {26, 7},
        .related = {"Npy", "Parquet"}});
}

void registerNetCDFSchemaReader(FormatFactory & factory);
void registerNetCDFSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("NetCDF", [](ReadBuffer & buf, const FormatSettings & settings)
    {
        return std::make_shared<NetCDFSchemaReader>(buf, settings);
    });
}

}
