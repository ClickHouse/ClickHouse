#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Columns/ColumnWithDictionary.h>
#include <DataTypes/DataTypeWithDictionary.h>

namespace DB
{


/** Combines several sources into one.
  * Unlike UnionBlockInputStream, it does this sequentially.
  * Blocks of different sources are not interleaved with each other.
  */
class ConvertColumnWithDictionaryToFullBlockInputStream : public IProfilingBlockInputStream
{
public:
    explicit ConvertColumnWithDictionaryToFullBlockInputStream(const BlockInputStreamPtr & input)
    {
        children.push_back(input);
    }

    String getName() const override { return "ConvertColumnWithDictionaryToFull"; }

    Block getHeader() const override { return convert(children.at(0)->getHeader()); }

protected:
    Block readImpl() override { return convert(children.back()->read()); }

private:
    Block convert(Block && block) const
    {
        for (auto & column : block)
        {
            auto * type_with_dict = typeid_cast<const DataTypeWithDictionary *>(column.type.get());
            auto * col_with_dict = typeid_cast<const ColumnWithDictionary *>(column.column.get());

            if (type_with_dict && !col_with_dict)
                throw Exception("Invalid column for " + type_with_dict->getName() + ": " + column.column->getName(),
                                ErrorCodes::LOGICAL_ERROR);

            if (!type_with_dict && col_with_dict)
                throw Exception("Invalid type for " + col_with_dict->getName() + ": " + column.type->getName(),
                                ErrorCodes::LOGICAL_ERROR);

            if (type_with_dict && col_with_dict)
            {
                column.column = col_with_dict->convertToFullColumn();
                column.type = type_with_dict->getDictionaryType();
            }
        }

        return std::move(block);
    }
};

}
