#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetConfigQuery.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/FieldVisitorToString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NO_FDB;
    extern const int FDB_META_EXCEPTION;
    extern const int UNKNOWN_CONFIG;
}

ConfigParamKVPtr InterpreterSetConfigQuery::getMetadataConfigFromQuery(const ASTSetConfigQuery & query)
{
    ConfigParamKVPtr kv_p(new FoundationDB::Proto::MetadataConfigParam());
    kv_p->set_name(query.name);
    if (!query.is_none)
    {
        if (query.value == true)
            kv_p->set_value("true");
        else if (query.value == false)
            kv_p->set_value("false");
        else kv_p->set_value(toString(query.value));
    }
    return kv_p;
}

BlockIO InterpreterSetConfigQuery::execute()
{
    const auto & query = query_ptr->as<const ASTSetConfigQuery &>();
    ConfigParamKVPtr temp = getMetadataConfigFromQuery(query);
    if (!getContext()->hasMetadataStoreFoundationDB())
    {
        throw Exception("There is no FoundationDB", ErrorCodes::NO_FDB);
    }
    std::shared_ptr<MetadataStoreFoundationDB> meta_store = getContext()->getMetadataStoreFoundationDB();

    auto global_context = Context::createCopy(getContext()->getGlobalContext());

    /// 1. Set global config
    if (query.is_global)
    {
        /// TODO: Set global config

    }
    /// 2. Set instance config
    else
    {
        /// If this config exists on fdb: update/remove
        if (meta_store->isExistsConfigParamMeta(query.name))
        {
            if (!query.is_none)
                meta_store->updateConfigParamMeta(*temp,temp->name());
            else
                meta_store->removeConfigParamMeta(query.name);
        }
        /// If this config doesn't exist on fdb: create/error
        else
        {
            if (!query.is_none)
                meta_store->addConfigParamMeta(*temp,temp->name());
            else
                throw Exception("Key '" + query.name +"' doesn't exist in FoundationDB", ErrorCodes::FDB_META_EXCEPTION);
        }
        global_context->reloadConfig();
    }
    return {};
}
}
