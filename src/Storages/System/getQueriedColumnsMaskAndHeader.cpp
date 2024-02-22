#include <Storages/System/getQueriedColumnsMaskAndHeader.h>

namespace DB
{

std::pair<std::vector<UInt8>, Block> getQueriedColumnsMaskAndHeader(const Block & sample_block, const Names & column_names)
{
    std::vector<UInt8> columns_mask(sample_block.columns());
    Block header;

    NameSet names_set(column_names.begin(), column_names.end());
    for (size_t i = 0; i < columns_mask.size(); ++i)
    {
        if (names_set.contains(sample_block.getByPosition(i).name))
        {
            columns_mask[i] = 1;
            header.insert(sample_block.getByPosition(i));
        }
    }

    return std::make_pair(columns_mask, header);
}

}
