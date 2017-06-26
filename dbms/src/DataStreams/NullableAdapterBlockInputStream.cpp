#include <DataStreams/NullableAdapterBlockInputStream.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataStreams/isConvertableTypes.h>


namespace DB
{

namespace ErrorCodes
{

extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
extern const int TYPE_MISMATCH;

}

NullableAdapterBlockInputStream::NullableAdapterBlockInputStream(
    BlockInputStreamPtr input_,
    const Block & in_sample_, const Block & out_sample_)
{
    buildActions(in_sample_, out_sample_);
    children.push_back(input_);
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
    size_t s = block.columns();

    for (size_t i = 0; i < s; ++i)
    {
        const auto & elem = block.getByPosition(i);

        switch (actions[i])
        {
            case TO_ORDINARY:
            {
                const auto & nullable_col = static_cast<const ColumnNullable &>(*elem.column);
                const auto & nullable_type = static_cast<const DataTypeNullable &>(*elem.type);

                const auto & null_map = nullable_col.getNullMap();
                bool has_nulls = !memoryIsZero(null_map.data(), null_map.size());

                if (has_nulls)
                    throw Exception{"Cannot insert NULL value into non-nullable column",
                        ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN};
                else
                    res.insert({
                        nullable_col.getNestedColumn(),
                        nullable_type.getNestedType(),
                        rename[i].value_or(elem.name)
                    });
                break;
            }
            case TO_NULLABLE:
            {
                auto null_map = std::make_shared<ColumnUInt8>(elem.column->size(), 0);

                res.insert({
                    std::make_shared<ColumnNullable>(elem.column, null_map),
                    std::make_shared<DataTypeNullable>(elem.type),
                    rename[i].value_or(elem.name)
                });
                break;
            }
            case NONE:
            {
                if (rename[i])
                    res.insert({elem.column, elem.type, rename[i].value()});
                else
                    res.insert(elem);
                break;
            }
        }
    }

    return res;
}

void NullableAdapterBlockInputStream::buildActions(
    const Block & in_sample,
    const Block & out_sample)
{
    size_t in_size = in_sample.columns();

    actions.reserve(in_size);
    rename.reserve(in_size);

    for (size_t i = 0; i < in_size; ++i)
    {
        const auto & in_elem  = in_sample.getByPosition(i);
        const auto & out_elem = out_sample.getByPosition(i);

        if (isConvertableTypes(in_elem.type, out_elem.type))
        {
            bool is_in_nullable = in_elem.type->isNullable();
            bool is_out_nullable = out_elem.type->isNullable();

            if (is_in_nullable && !is_out_nullable)
                actions.push_back(TO_ORDINARY);
            else if (!is_in_nullable && is_out_nullable)
                actions.push_back(TO_NULLABLE);
            else
                actions.push_back(NONE);

            if (in_elem.name != out_elem.name)
                rename.push_back(std::experimental::make_optional(out_elem.name));
            else
                rename.push_back(std::experimental::nullopt);

            if (actions.back() != NONE || rename.back())
                must_transform = true;
        }
        else
        {
            throw Exception{String("Types must be the same for columns at same position. ")
                + "Column " + in_elem.name + " has type " + in_elem.type->getName()
                + ", but column " + out_elem.name + " has type " + out_elem.type->getName(),
                ErrorCodes::TYPE_MISMATCH};
        }
    }
}

}
