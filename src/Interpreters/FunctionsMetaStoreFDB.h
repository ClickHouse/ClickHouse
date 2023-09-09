#pragma once
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExternalLoader.h>
#include <Interpreters/ExternalLoaderFDBFunctionConfigRepository.h>
#include <Interpreters/ExternalLoaderXMLConfigRepository.h>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <boost/noncopyable.hpp>
#include <Poco/Logger.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/FoundationDB/MetadataStoreFoundationDB.h>
#include <Common/logger_useful.h>


namespace DB
{
using Repository = IExternalLoaderConfigRepository;
using FileInfo = ExternalLoader::FileInfo;
using RepositoryInfo = ExternalLoader::RepositoryInfo;
using XmlFuncInfo = FoundationDB::Proto::XmlFuncInfo;
using XMLConfiguration = Poco::Util::XMLConfiguration;
class FunctionsMetaStoreFDB : public boost::noncopyable, WithContext
{
public:
    explicit FunctionsMetaStoreFDB(ContextPtr context_);
    Poco::Timestamp getUpdateTime(const std::string & func_name);
    bool addOneFunc(ExternalLoaderXMLConfigRepository * repository, const std::unordered_map<Repository *, RepositoryInfo> & repositories);
    bool deleteOneFunc(const std::string & func_name);
    bool exist(const std::string & func_name) const;
    Poco::AutoPtr<Poco::Util::AbstractConfiguration> convertStringToConfig(std::string config_str);
    RepositoryInfo getOneFunc(const std::string & func_name);
    DB::LoadablesConfigurationPtr getOneFuncConfig(const std::string & func_name);
    std::vector<std::string> list();
    /// Get all functions from foundationdb.
    std::vector<std::unique_ptr<Repository>> getAllFunc();
    bool isFirstOpen();

private:
    std::mutex meta_store_mutex;
    std::shared_ptr<MetadataStoreFoundationDB> meta_store;
    void createRepositoryInfoFromFuncInfo(RepositoryInfo & repo_info, XmlFuncInfo func);
    void createRepositoriesFromFuncs(
        std::unordered_map<std::string, XmlFuncInfo> funcs, std::unordered_map<Repository *, RepositoryInfo> & repositories);
    std::vector<std::string> split(std::string & str, std::string & pattern);
    std::vector<std::string> splitXML(std::string & xml);
    std::string convertConfigToString(Poco::AutoPtr<Poco::Util::AbstractConfiguration> config);
    Poco::Logger *log;
};
}
