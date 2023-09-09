#pragma once
#include <mutex>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExternalLoaderDictionaryStorageConfigRepository.h>
#include <Interpreters/ExternalLoaderFDBDictionaryConfigRepository.h>
#include <Interpreters/ExternalLoaderXMLConfigRepository.h>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <boost/noncopyable.hpp>
#include <Poco/Logger.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/FoundationDB/MetadataStoreFoundationDB.h>
#include <Common/FoundationDB/protos/MetadataDictionaries.pb.h>
#include <Common/logger_useful.h>
#include "Interpreters/ExternalLoader.h"


namespace DB
{
class DictionariesMetaStoreFDB : public boost::noncopyable, WithContext
{
public:
    using DictInfo = FoundationDB::Proto::DictInfo;
    using MetadataDictionaries = FoundationDB::Proto::MetadataDictionaries;
    using XMLConfiguration = Poco::Util::XMLConfiguration;
    using Repository = IExternalLoaderConfigRepository;
    using RepositoryInfo = ExternalLoader::RepositoryInfo;
    using FileInfo = ExternalLoader::FileInfo;
    explicit DictionariesMetaStoreFDB(ContextPtr context_);
    Poco::Timestamp getUpdateTime(const std::string & dict_name);
    bool addOneDict(ExternalLoaderXMLConfigRepository * repository, std::unordered_map<Repository *, RepositoryInfo> & repositories);
    bool add(DictInfo & new_dict);

    bool deleteOneDict(const std::string & dict_name);
    bool exist(const std::string & dict_name) const;
    Poco::AutoPtr<Poco::Util::AbstractConfiguration> convertStringToConfig(std::string config_str);
    RepositoryInfo getOneDict(const std::string & dict_name);
    DB::LoadablesConfigurationPtr getOneDictConfig(const std::string & dict_name);
    std::vector<std::string> list();
    /// Get all dictionaries from foundationdb.
    std::unordered_map<Repository *, RepositoryInfo> getAllDict();
    std::vector<std::unique_ptr<Repository>> getAllDictPtr();
    bool isFirstOpen();

private:
    std::mutex meta_store_mutex;
    std::shared_ptr<MetadataStoreFoundationDB> meta_store;
    void createRepositoryInfoFromDictInfo(RepositoryInfo & repo_info, DictInfo dict);
    void createRepositoriesFromDicts(
        std::unordered_map<std::string, DictInfo> dicts, std::unordered_map<Repository *, RepositoryInfo> & repositories);
    std::vector<std::string> split(std::string str, std::string pattern);
    std::vector<std::string> splitXml(std::string xml);
    std::string convertConfigToString(Poco::AutoPtr<Poco::Util::AbstractConfiguration> config);
    Poco::Logger * log;
};

}
