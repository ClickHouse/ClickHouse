#pragma once

#include <Processors/ISimpleTransform.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{

class ReplacingWindowColumnTransform : public ISimpleTransform
{
public:
    ReplacingWindowColumnTransform(
        const Block & header_,
        size_t window_id_column_pos_,
        const NameAndTypePair & window_column_name_and_type_,
        Tuple window_value_)
        : ISimpleTransform(header_, getResultHeader(header_, window_id_column_pos_), false)
        , window_id_column_pos(window_id_column_pos_)
        , window_column_name_and_type(window_column_name_and_type_)
        , window_value(window_value_)
    {
        replaced_window_id_column_pos = header_.getPositionByName(window_column_name_and_type.name);
    }

    String getName() const override { return "ReplacingWindowColumnTransform"; }

    static Block getResultHeader(Block header, size_t window_id_column_pos_)
    {
        header.erase(window_id_column_pos_);
        return header;
    }

protected:
    void transform(Chunk & chunk) override
    {
        auto window_column = window_column_name_and_type.type->createColumnConst(
            chunk.getNumRows(), window_value)->convertToFullColumnIfConst();

        chunk.erase(replaced_window_id_column_pos);
        chunk.addColumn(replaced_window_id_column_pos, window_column);

        chunk.erase(window_id_column_pos);
        chunk.setChunkInfo(std::make_shared<AggregatedChunkInfo>());
    }

    size_t window_id_column_pos;
    size_t replaced_window_id_column_pos;

    NameAndTypePair window_column_name_and_type;
    Tuple window_value;
};

}
