#include <DataStreams/NullableAdapterBlockInputStream.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}

NullableAdapterBlockInputStream::NullableAdapterBlockInputStream(
    const BlockInputStreamPtr & input,
    const Block & in_sample_, const Block & out_sample_)
{
    buildActions(in_sample_, out_sample_);
    children.push_back(input);
}

String NullableAdapterBlockInputStream::getID() const
{
    std::stringstream res;
    res << "NullableAdapterBlockInputStream(" << children.back()->getID() << ")";
    return res.str();
}

Block NullableAdapterBlockInputStream::readImpl()
{
    Block block = children.back()->read();

    if (!block && !must_transform)
        return block;

    Block res;

    for (size_t i = 0, s = block.columns(); i < s; ++i)
        actions[i].execute(block, res);

    return res;
}

void NullableAdapterBlockInputStream::buildActions(
    const Block & in_sample,
    const Block & out_sample)
{
    size_t out_size = out_sample.columns();

    /// TODO: Maybe remove this when have default value in the column.
    if (in_sample.columns() != out_size)
        throw Exception("Number of columns in INSERT SELECT doesn't match", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

    actions.reserve(out_size);

    bool buildWithName = true;

    for (size_t i = 0; i < out_size; ++i)
    {
        const auto &out_elem = out_sample.getByPosition(i);
        if (!in_sample.has(out_elem.name))
        {
            buildWithName = false;
            break;
        }
    }

    for (size_t i = 0; i < out_size; ++i)
    {
        const auto & out_elem = out_sample.getByPosition(i);
        const auto & in_elem = buildWithName ? in_sample.getByName(out_elem.name) : in_sample.getByPosition(i);

        bool is_in_nullable = in_elem.type->isNullable();
        bool is_out_nullable = out_elem.type->isNullable();

        if (is_in_nullable && !is_out_nullable)
            actions.push_back(
            Action::create(Action::TO_ORDINARY,
                           buildWithName ? in_sample.getPositionByName(out_elem.name) : i,
                           out_elem.name));
        else if (!is_in_nullable && is_out_nullable)
            actions.push_back(
            Action::create(Action::TO_NULLABLE,
                           buildWithName ? in_sample.getPositionByName(out_elem.name) : i,
                           out_elem.name));
        else
            actions.push_back(
            Action::create(Action::NONE,
                           buildWithName ? in_sample.getPositionByName(out_elem.name) : i,
                           out_elem.name));

        if (actions.back().type != Action::NONE || in_elem.name != out_elem.name)
            must_transform = true;
    }
}

Action Action::create(Type type, size_t position, String new_name)
{
    Action res;
    res.type = type;
    res.position = position;
    res.new_name = new_name;

    return res;
}

void Action::execute(Block &old_block, Block &new_block)
{
    const auto & elem = old_block.getByPosition(position);
    switch (type)
    {
        case TO_NULLABLE:
        {
            ColumnPtr null_map = ColumnUInt8::create(elem.column->size(), 0);
            new_block.insert({ColumnNullable::create(elem.column, null_map), std::make_shared<DataTypeNullable>(elem.type), new_name});
            break;
        }
        case TO_ORDINARY:
        {
            const auto & nullable_col = static_cast<const ColumnNullable &>(*elem.column);
            const auto & nullable_type = static_cast<const DataTypeNullable &>(*elem.type);

            const auto & null_map = nullable_col.getNullMapData();
            const auto has_nulls = !memoryIsZero(null_map.data(), null_map.size());

            if (has_nulls)
                throw Exception("Cannot insert NULL value into non-nullable column",
                                ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN);

            new_block.insert({nullable_col.getNestedColumnPtr(), nullable_type.getNestedType(), new_name});
            break;
        }
        case NONE:
            new_block.insert({elem.column, elem.type, new_name});
            break;
    }
}

}
