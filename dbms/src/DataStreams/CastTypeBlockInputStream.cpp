#include <DataStreams/CastTypeBlockInputStream.h>
#include <Interpreters/castColumn.h>


namespace DB
{


CastTypeBlockInputStream::CastTypeBlockInputStream(
    const Context & context_,
    const BlockInputStreamPtr & input,
    const Block & reference_definition)
    : context(context_)
{
    children.emplace_back(input);

    Block input_header = input->getHeader();

    for (size_t col_num = 0, num_columns = input_header.columns(); col_num < num_columns; ++col_num)
    {
        const auto & elem = input_header.getByPosition(col_num);

        /// Skip, if it is a problem, it will be detected on the next pipeline stage
        if (!reference_definition.has(elem.name))
            continue;

        const auto & ref_column = reference_definition.getByName(elem.name);

        /// Force conversion if source and destination types is different.
        if (ref_column.type->equals(*elem.type))
        {
            header.insert(elem);
        }
        else
        {
            header.insert({ castColumn(elem, ref_column.type, context), ref_column.type, elem.name });
            cast_description.emplace(col_num, ref_column.type);
        }
    }
}

String CastTypeBlockInputStream::getName() const
{
    return "CastType";
}

Block CastTypeBlockInputStream::readImpl()
{
    Block block = children.back()->read();

    if (!block)
        return block;

    if (cast_description.empty())
        return block;

    size_t num_columns = block.columns();
    Block res = block;

    for (size_t col = 0; col < num_columns; ++col)
    {
        auto it = cast_description.find(col);
        if (cast_description.end() != it)
        {
            auto & elem = res.getByPosition(col);
            elem.column = castColumn(elem, it->second, context);
            elem.type = it->second;
        }
    }

    return res;
}

}
