#include <iostream>
#include <Interpreters/Context.h>

int main()
{
    using namespace DB;
    auto shared_context = Context::createShared();
    auto global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    global_context->initializeMergeTreeMetadataCache("./db/", 256 << 20);

    auto cache = global_context->getMergeTreeMetadataCache();

    std::vector<String> files
        = {"columns.txt", "checksums.txt", "primary.idx", "count.txt", "partition.dat", "minmax_p.idx", "default_compression_codec.txt"};
    String prefix = "data/test_metadata_cache/check_part_metadata_cache/201806_1_1_0_4/";

    for (const auto & file : files)
    {
        auto status = cache->put(prefix + file, prefix + file);
        std::cout << "put " << file << " " << status.ToString() << std::endl;
    }

    for (const auto & file : files)
    {
        String value;
        auto status = cache->get(prefix + file, value);
        std::cout << "get " << file << " " << status.ToString() << " " << value << std::endl;
    }


    for (const auto & file : files)
    {
        auto status = cache->del(prefix + file);
        std::cout << "del " << file << " " << status.ToString() << std::endl;
    }

    for (const auto & file : files)
    {
        String value;
        auto status = cache->get(prefix + file, value);
        std::cout << "get " << file << " " << status.ToString() << " " << value << std::endl;
    }

    Strings keys;
    Strings values;
    cache->getByPrefix(prefix, keys, values);
    for (size_t i=0; i<keys.size(); ++i)
        std::cout << "get key:" << keys[i] << " value:" << values[i] << std::endl;
    return 0;
}
