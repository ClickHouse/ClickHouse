#include <DataTypes/DataTypeMap.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemConfigs.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/FoundationDB/protos/MetadataConfigParam.pb.h>

namespace DB
{
using XMLDocumentPtr = Poco::AutoPtr<Poco::XML::Document>;
using ConfigParamKV = FoundationDB::Proto::MetadataConfigParam;
using ConfigParamKVs = std::vector<std::unique_ptr<ConfigParamKV>>;

NamesAndTypesList StorageSystemConfigs::getNamesAndTypes()
{
    return
    {
        {"name", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemConfigs::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    if (!context->hasMetadataStoreFoundationDB())
    {
        LOG_INFO(&Poco::Logger::root(), "The table 'system.configs' is only useful when foundationDB is configured.");
    }
    else
    {
        auto & config = context->getXmlConfigRef();
        Poco::XML::Node * node = config->getNodeByPath("clickhouse");
        if (node == nullptr)
            node = config->getNodeByPath("yandex");
        ConfigParamKVs kvs;
        ConfigProcessor::transformXMLToKV(node, "", kvs, false);
        for (const auto & kv : kvs)
        {
            // Configurations prefixed with merge_tree, remote_servers, macros, storage_configuration are already shown in other system tables.
            std::string prefix = kv->name().substr(0, kv->name().find('.'));
            if (prefix == "merge_tree" || prefix == "remote_servers" || prefix == "macros" || prefix == "storage_configuration")
                continue;

            res_columns[0]->insert(kv->name());
            res_columns[1]->insert(kv->value());
        }
    }
}

}
