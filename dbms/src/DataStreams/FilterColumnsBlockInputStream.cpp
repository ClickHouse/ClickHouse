#include <DataStreams/FilterColumnsBlockInputStream.h>

namespace DB
{

String FilterColumnsBlockInputStream::getID() const
{
    std::stringstream res;
    res << "FilterColumnsBlockInputStream(" << children.back()->getID();

    for (const auto & it : columns_to_save)
        res << ", " << it;

    res << ")";
    return res.str();
}

Block FilterColumnsBlockInputStream::readImpl()
{
    Block block = children.back()->read();

    if (!block)
        return block;

    Block filtered;

    for (const auto & it : columns_to_save)
        filtered.insert(std::move(block.getByName(it)));

    return filtered;
}

}
