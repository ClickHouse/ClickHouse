#include <iostream>
#include <base/UUID.h>
#include <fmt/ranges.h>
#include <Common/FoundationDB/MetadataStoreFoundationDB.h>
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/Exception.h>
#include "MergeTreePartInfo.pb.h"
#include "common.h"

using namespace DB;
using namespace DB::FoundationDB;

int main(int argc, char ** argv)
try
{
    const char * config_file = "/etc/clickhouse-server/config.xml";
    if (argc == 2)
    {
        config_file = argv[1];
    }
    else if (argc > 2)
    {
        std::cerr << "Usage: " << argv[0] << " [config_file]" << std::endl;
        return 1;
    }
    std::cerr << "Using config: " << config_file << std::endl;

    initLogger();
    auto meta_store = createMetadataStore(config_file);

    Proto::MergeTreePartDiskMeta disk_meta;
    Proto::MergeTreePartMeta part_meta;
    PartKey key{DB::UUID({1, 1}), "table"};

    std::string very_long_str(203'000, 'a');
    part_meta.set_meta_name(very_long_str);

    meta_store->addPartMeta(disk_meta, part_meta, key);
    // std::cerr << meta_store->isExistsPart(key) << std::endl;
    // part_meta = *meta_store->getPartMeta(key);
    // meta_store->removePartMeta(key);
    // std::cerr << meta_store->isExistsPart(key) << std::endl;

    FoundationDBNetwork::shutdownIfNeed();
    return 0;
}
catch (...)
{
    tryLogCurrentException(__PRETTY_FUNCTION__);
    FoundationDBNetwork::shutdownIfNeed();
    return 1;
}
