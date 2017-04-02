#include <set>

#include <DataStreams/glueBlockInputStreams.h>


namespace DB
{


using IDsMap = std::map<String, BlockInputStreams>;
using ForksMap = std::map<String, ForkPtr>;


static void createIDsMap(BlockInputStreamPtr & node, IDsMap & ids_map)
{
    ids_map[node->getID()].push_back(node);

    BlockInputStreams & children = node->getChildren();
    for (size_t i = 0, size = children.size(); i < size; ++i)
        createIDsMap(children[i], ids_map);
}


static void glue(BlockInputStreamPtr & node, IDsMap & ids_map, ForksMap & forks_map)
{
    String id = node->getID();
    if (ids_map.end() != ids_map.find(id) && ids_map[id].size() > 1)
    {
        /// Insert a "fork" or use an existing one.
        if (forks_map.end() == forks_map.find(id))
        {
            forks_map[id] = std::make_shared<ForkBlockInputStreams>(node);
            std::cerr << "Forking at " << id << std::endl;
        }

        std::cerr << "Replacing node with fork end" << std::endl;
        node = forks_map[id]->createInput();
    }
    else
    {
        BlockInputStreams & children = node->getChildren();
        for (size_t i = 0, size = children.size(); i < size; ++i)
            glue(children[i], ids_map, forks_map);
    }
}


void glueBlockInputStreams(BlockInputStreams & inputs, Forks & forks)
{
    IDsMap ids_map;
    for (size_t i = 0, size = inputs.size(); i < size; ++i)
        createIDsMap(inputs[i], ids_map);

    ForksMap forks_map;
    for (size_t i = 0, size = inputs.size(); i < size; ++i)
        glue(inputs[i], ids_map, forks_map);

    for (ForksMap::iterator it = forks_map.begin(); it != forks_map.end(); ++it)
        forks.push_back(it->second);
}


}
