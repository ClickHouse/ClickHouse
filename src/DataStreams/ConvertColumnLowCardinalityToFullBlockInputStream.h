#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnConst.h>

namespace DB
{


/** Combines several sources into one.
  * Unlike UnionBlockInputStream, it does this sequentially.
  * Blocks of different sources are not interleaved with each other.
  */
class ConvertColumnLowCardinalityToFullBlockInputStream : public IBlockInputStream
{
public:
    explicit ConvertColumnLowCardinalityToFullBlockInputStream(const BlockInputStreamPtr & input)
    {
        children.push_back(input);
    }

    String getName() const override { return "ConvertColumnLowCardinalityToFull"; }

    Block getHeader() const override { return convert(children.at(0)->getHeader()); }

protected:
    Block readImpl() override { return convert(children.back()->read()); }

private:
    Block convert(Block && block) const
    {
        for (auto & column : block)
        {
            if (auto * column_const = typeid_cast<const ColumnConst *>(column.column.get()))
                column.column = column_const->removeLowCardinality();
            else
                column.column = column.column->convertToFullColumnIfLowCardinality();

            if (auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(column.type.get()))
                column.type = low_cardinality_type->getDictionaryType();
        }

        return std::move(block);
    }
};

}
