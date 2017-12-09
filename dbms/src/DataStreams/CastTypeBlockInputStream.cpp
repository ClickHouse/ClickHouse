#include <DataStreams/CastTypeBlockInputStream.h>
#include <Interpreters/castColumn.h>


namespace DB
{


CastTypeBlockInputStream::CastTypeBlockInputStream(
    const Context & context_,
    const BlockInputStreamPtr & input_,
    const Block & reference_definition_)
    : context(context_), ref_definition(reference_definition_)
{
    children.emplace_back(input_);
}

String CastTypeBlockInputStream::getName() const
{
    return "CastType";
}

String CastTypeBlockInputStream::getID() const
{
    return "CastType(" + children.back()->getID() + ")";
}

Block CastTypeBlockInputStream::readImpl()
{
    Block block = children.back()->read();

    if (!block)
        return block;

    if (!initialized)
    {
        initialized = true;
        initialize(block);
    }

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


void CastTypeBlockInputStream::initialize(const Block & src_block)
{
    for (size_t src_col = 0, num_columns = src_block.columns(); src_col < num_columns; ++src_col)
    {
        const auto & src_column = src_block.getByPosition(src_col);

        /// Skip, if it is a problem, it will be detected on the next pipeline stage
        if (!ref_definition.has(src_column.name))
            continue;

        const auto & ref_column = ref_definition.getByName(src_column.name);

        /// Force conversion if source and destination types is different.
        if (!ref_column.type->equals(*src_column.type))
            cast_description.emplace(src_col, ref_column.type);
    }
}

}
