#include <DataStreams/CastEnumBlockInputStream.h>
#include <DataTypes/DataTypeEnum.h>

namespace DB
{

CastEnumBlockInputStream::CastEnumBlockInputStream(
    BlockInputStreamPtr input_,
    const Block & in_sample_,
    const Block & out_sample_)
{
    collectEnums(in_sample_, out_sample_);
    children.push_back(input_);
}

String CastEnumBlockInputStream::getName() const
{
    return "CastEnum";
}

String CastEnumBlockInputStream::getID() const
{
    std::stringstream res;
    res << "CastEnum(" << children.back()->getID() << ")";
    return res.str();
}

Block CastEnumBlockInputStream::readImpl()
{
    Block block = children.back()->read();

    if (!block || enum_types.empty())
        return block;

    Block res;
    size_t s = block.columns();

    for (size_t i = 0; i < s; ++i)
    {
        const auto & elem = block.getByPosition(i);

        if (bool(enum_types[i]))
        {
            const auto & type = static_cast<const IDataTypeEnum *>(enum_types[i]->type.get());
            ColumnPtr new_column = type->createColumn();

            for (size_t j = 0; j < elem.column->size(); ++j)
                new_column->insert(type->castToValue((*elem.column)[j]));

            res.insert({
                new_column,
                enum_types[i]->type,
                enum_types[i]->name});
        }
        else
        {
            res.insert(elem);
        }
    }

    return res;
}

void CastEnumBlockInputStream::collectEnums(const Block & in_sample, const Block & out_sample)
{
    size_t in_size = in_sample.columns();
    enum_types.resize(in_size);
    for (size_t i = 0; i < in_size; ++i)
    {
        const auto & in_elem  = in_sample.getByPosition(i);
        const auto & out_elem = out_sample.getByPosition(i);

        /// Force conversion only if source type is not Enum.
        if ( dynamic_cast<IDataTypeEnum*>(out_elem.type.get()) &&
            !dynamic_cast<IDataTypeEnum*>(in_elem.type.get()))
        {
            enum_types[i] = NameAndTypePair(out_elem.name, out_elem.type);
        }
    }
}

}
