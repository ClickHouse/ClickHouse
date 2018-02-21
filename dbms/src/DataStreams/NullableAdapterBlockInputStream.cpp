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


static Block transform(const Block & block, const NullableAdapterBlockInputStream::Actions & actions, const std::vector<std::optional<String>> & rename)
{
    size_t num_columns = block.columns();

    Block res;
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & elem = block.getByPosition(i);

        switch (actions[i])
        {
            case NullableAdapterBlockInputStream::TO_ORDINARY:
            {
                const auto & nullable_col = static_cast<const ColumnNullable &>(*elem.column);
                const auto & nullable_type = static_cast<const DataTypeNullable &>(*elem.type);

                const auto & null_map = nullable_col.getNullMapData();
                bool has_nulls = !memoryIsZero(null_map.data(), null_map.size());

                if (has_nulls)
                    throw Exception{"Cannot insert NULL value into non-nullable column",
                        ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN};
                else
                    res.insert({
                        nullable_col.getNestedColumnPtr(),
                        nullable_type.getNestedType(),
                        rename[i].value_or(elem.name)});
                break;
            }
            case NullableAdapterBlockInputStream::TO_NULLABLE:
            {
                ColumnPtr null_map = ColumnUInt8::create(elem.column->size(), 0);

                res.insert({
                    ColumnNullable::create(elem.column, null_map),
                    std::make_shared<DataTypeNullable>(elem.type),
                    rename[i].value_or(elem.name)});
                break;
            }
            case NullableAdapterBlockInputStream::NONE:
            {
                res.insert({elem.column, elem.type, rename[i].value_or(elem.name)});
                break;
            }
        }
    }

    return res;
}


NullableAdapterBlockInputStream::NullableAdapterBlockInputStream(
    const BlockInputStreamPtr & input,
    const Block & src_header, const Block & res_header)
{
    buildActions(src_header, res_header);
    children.push_back(input);
    header = transform(src_header, actions, rename);
}


Block NullableAdapterBlockInputStream::readImpl()
{
    Block block = children.back()->read();

    if (!block)
        return block;

    return transform(block, actions, rename);
}

void NullableAdapterBlockInputStream::buildActions(
    const Block & src_header,
    const Block & res_header)
{
    size_t in_size = src_header.columns();

    if (res_header.columns() != in_size)
        throw Exception("Number of columns in INSERT SELECT doesn't match", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

    actions.reserve(in_size);
    rename.reserve(in_size);

    for (size_t i = 0; i < in_size; ++i)
    {
        const auto & in_elem  = src_header.getByPosition(i);
        const auto & out_elem = res_header.getByPosition(i);

        bool is_in_nullable = in_elem.type->isNullable();
        bool is_out_nullable = out_elem.type->isNullable();

        if (is_in_nullable && !is_out_nullable)
            actions.push_back(TO_ORDINARY);
        else if (!is_in_nullable && is_out_nullable)
            actions.push_back(TO_NULLABLE);
        else
            actions.push_back(NONE);

        if (in_elem.name != out_elem.name)
            rename.emplace_back(std::make_optional(out_elem.name));
        else
            rename.emplace_back();
    }
}

}
