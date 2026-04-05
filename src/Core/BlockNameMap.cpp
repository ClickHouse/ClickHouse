#include <Core/BlockNameMap.h>
#include <Core/Block.h>

namespace DB
{

BlockNameMap getNamesToIndexesMap(const Block & block)
{
    auto const & index_by_name = block.getIndexByName();

    BlockNameMap res(index_by_name.size());
    res.set_empty_key(StringRef{});
    for (const auto & [name, index] : index_by_name)
        res[name] = index;
    return res;
}

}
