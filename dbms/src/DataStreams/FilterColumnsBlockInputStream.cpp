#include <DataStreams/FilterColumnsBlockInputStream.h>

namespace DB
{

Block FilterColumnsBlockInputStream::getHeader() const
{
    Block block = children.back()->getHeader();
    Block filtered;

    for (const auto & it : columns_to_save)
        if (throw_if_column_not_found || block.has(it))
            filtered.insert(std::move(block.getByName(it)));

    return filtered;
}

Block FilterColumnsBlockInputStream::readImpl()
{
    Block block = children.back()->read();

    if (!block)
        return block;

    Block filtered;

    for (const auto & it : columns_to_save)
        if (throw_if_column_not_found || block.has(it))
            filtered.insert(std::move(block.getByName(it)));

    return filtered;
}

}
