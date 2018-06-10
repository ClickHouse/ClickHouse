#pragma once

#include <unordered_map>
#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Convert one block structure to another:
  *
  * Leaves only necessary columns;
  *
  * Columns are searched in source first by name;
  *  and if there is no column with same name, then by position.
  *
  * Converting types of matching columns (with CAST function).
  *
  * Materializing columns which are const in source and non-const in result,
  *  throw if they are const in result and non const in source,
  *   or if they are const and have different values.
  */
class ConvertingBlockInputStream : public IProfilingBlockInputStream
{
public:
    enum class MatchColumnsMode
    {
        /// Require same number of columns in source and result. Match columns by corresponding positions, regardless to names.
        Position,
        /// Find columns in source by their names. Allow excessive columns in source.
        Name
    };

    ConvertingBlockInputStream(
        const Context & context,
        const BlockInputStreamPtr & input,
        const Block & result_header,
        MatchColumnsMode mode);

    String getName() const override { return "Converting"; }
    Block getHeader() const override { return header; }

private:
    Block readImpl() override;

    const Context & context;
    Block header;

    /// How to construct result block. Position in source block, where to get each column.
    using Conversion = std::vector<size_t>;
    Conversion conversion;
};

}
