#pragma once

#include <Columns/IColumn.h>
#include <Formats/NetCDF.h>
#include <Processors/Formats/IOutputFormat.h>

#include <vector>

namespace DB
{

/// Writes the data as a NetCDF classic file. Every column becomes a one-dimensional variable over a
/// single dimension named `row`, and a String column additionally gets a dimension that holds the
/// length of the longest string in it.
///
/// The offsets of the data of the variables are a part of the header, and the header is at the
/// beginning of the file, so the whole result is kept in memory until it is written out.
class NetCDFOutputFormat final : public IOutputFormat
{
public:
    NetCDFOutputFormat(WriteBuffer & out_, SharedHeader header_);

    String getName() const override { return "NetCDFOutputFormat"; }

private:
    struct Variable
    {
        String name;
        NetCDFType type = NetCDFType::Double;
        /// A `char` variable, which is how a String column is stored.
        bool is_string = false;
        /// The index of the dimension that holds the length of the strings.
        size_t string_dimension_id = 0;
        UInt64 string_length = 0;
        /// The value written in place of a NULL, in the representation of the file.
        String fill_value;
        /// The value of the `units` attribute, which tells what the numbers of a column with dates
        /// or times mean. Empty when the column has no such meaning.
        String units;
        /// What the values of a `DateTime64` column are multiplied by on writing. The CF
        /// conventions name only the units of the scales 0, 3, 6 and 9, so a column of another
        /// scale is written in the next finer named unit.
        Int64 time_multiplier = 1;

        /// The data of the column, collected from all the chunks.
        MutableColumnPtr data;
        /// Where NULLs are, for a column that has them.
        MutableColumnPtr null_map;

        /// The number of bytes of one value.
        UInt64 element_size = 0;
        /// The number of bytes of the whole data of the variable.
        UInt64 size = 0;
        UInt64 begin = 0;
    };

    void consume(Chunk chunk) override;
    void finalizeImpl() override;

    void writeHeader(WriteBuffer & buffer) const;
    void writeVariableData(const Variable & variable) const;

    std::vector<Variable> variables;
    std::vector<String> dimension_names;
    std::vector<UInt64> dimension_lengths;
    UInt64 num_rows = 0;
    UInt8 version = 2;
};

}
