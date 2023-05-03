#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DictionariesMetaStoreFDB.h>
#include <Interpreters/ExternalLoaderXMLConfigRepository.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/Exception.h>
namespace DB
{
using Repository = IExternalLoaderConfigRepository;
using RepositoryInfo = ExternalLoader::RepositoryInfo;
using FileInfo = ExternalLoader::FileInfo;
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
DictionariesMetaStoreFDB::DictionariesMetaStoreFDB(ContextPtr context_)
    : WithContext(context_), meta_store(context_->getMetadataStoreFoundationDB()), log(&Poco::Logger::get("DictionaryMetaStoreFDB"))
{
}

Poco::Timestamp DictionariesMetaStoreFDB::getUpdateTime(const std::string & dict_name)
{
    time_t update_time = static_cast<time_t>(meta_store->getOneDictionary(dict_name)->last_update_time());
    return Poco::Timestamp::fromEpochTime(update_time);
}

bool DictionariesMetaStoreFDB::addOneDict(
    ExternalLoaderXMLConfigRepository * repository, std::unordered_map<Repository *, RepositoryInfo> & repositories)
{
    const std::lock_guard lock(meta_store_mutex);
    if (repository == nullptr)
        return false;
    std::unordered_map<std::string /* path */, FileInfo> files = repositories.at(repository).files;
    for (auto & iter : files)
    {
        FileInfo file = iter.second;
        std::string file_contents = convertConfigToString(file.file_contents);
        /// Splitting string by '<dictionary>' to get all dictionaries.
        std::vector<std::string> dicts_xml = splitXml(file_contents);
        for (auto xml : dicts_xml)
        {
            auto new_config = convertStringToConfig(xml);
            std::string dict_name = new_config->getString("dictionary.name");
            if (meta_store->existOneDictionary(dict_name))
            {
                continue;
            }
            DictInfo dict;
            dict.set_dict_name(dict_name);
            dict.set_file_contents(xml);
            dict.set_name(dict_name);
            meta_store->addOneDictionary(dict, dict_name);
        }
    }
    return true;
}


bool DictionariesMetaStoreFDB::add(DictInfo & new_dict)
{
    const std::lock_guard<std::mutex> lock(meta_store_mutex);
    return meta_store->addOneDictionary(new_dict, new_dict.dict_name());
}

bool DictionariesMetaStoreFDB::deleteOneDict(const std::string & dict_name)
{
    const std::lock_guard<std::mutex> lock(meta_store_mutex);
    return meta_store->deleteOneDictionary(dict_name);
}

bool DictionariesMetaStoreFDB::exist(const std::string & dict_name) const
{
    return meta_store->existOneDictionary(dict_name);
}

RepositoryInfo DictionariesMetaStoreFDB::getOneDict(const std::string & dict_name)
{
    std::lock_guard<std::mutex> lock(meta_store_mutex);
    std::unique_ptr<DictInfo> dict = meta_store->getOneDictionary(dict_name);
    RepositoryInfo repository_info;

    createRepositoryInfoFromDictInfo(repository_info, *dict);
    return repository_info;
}

DB::LoadablesConfigurationPtr DictionariesMetaStoreFDB::getOneDictConfig(const std::string & dict_name)
{
    std::lock_guard<std::mutex> lock(meta_store_mutex);
    std::unique_ptr<DictInfo> fdb_dict = meta_store->getOneDictionary(dict_name);
    return convertStringToConfig(fdb_dict->file_contents());
}

/// Convert the repositories from fdb into map
std::unordered_map<Repository *, RepositoryInfo> DictionariesMetaStoreFDB::getAllDict()
{
    std::lock_guard<std::mutex> lock(meta_store_mutex);
    std::unordered_map<String, DictInfo> dicts;
    std::vector<std::unique_ptr<DictInfo>> fdb_dicts = meta_store->getAllDictionaries();
    for (auto & fdb_dict : fdb_dicts)
    {
        DictInfo dict = *fdb_dict;
        dicts.emplace(dict.dict_name(), dict);
    }
    std::unordered_map<Repository *, RepositoryInfo> repositories;
    createRepositoriesFromDicts(dicts, repositories);
    return repositories;
}
std::vector<std::unique_ptr<Repository>> DictionariesMetaStoreFDB::getAllDictPtr()
{
    std::lock_guard<std::mutex> lock(meta_store_mutex);
    std::vector<std::unique_ptr<Repository>> repos;
    std::vector<std::unique_ptr<DictInfo>> fdb_dicts = meta_store->getAllDictionaries();
    for (auto & fdb_dict : fdb_dicts)
    {
        auto repo = std::make_unique<ExternalLoaderFDBDictionaryConfigRepository>(fdb_dict->dict_name(), getContext());
        repos.push_back(std::move(repo));
    }
    return repos;
}
bool DictionariesMetaStoreFDB::isFirstOpen()
{
    std::lock_guard<std::mutex> lock(meta_store_mutex);
    return meta_store->isFirstBoot();
}
std::vector<std::string> DictionariesMetaStoreFDB::list()
{
    std::lock_guard<std::mutex> lock(meta_store_mutex);
    std::vector<std::unique_ptr<DictInfo>> dictionaries = meta_store->getAllDictionaries();
    std::vector<std::string> dictionary_names;
    dictionary_names.reserve(dictionaries.size());
    for (auto & dictionary : dictionaries)
    {
        dictionary_names.push_back(dictionary->dict_name());
    }
    return dictionary_names;
}


/// Tools to split string.
std::vector<std::string> DictionariesMetaStoreFDB::split(std::string str, std::string pattern)
{
    int pos;
    std::vector<std::string> result;
    str += pattern; /// Extend string for easy operation
    int size = static_cast<int>((str).size());
    for (int i = 0; i < size; i++)
    {
        pos = static_cast<int>(str.find(pattern, i));
        if (pos < size)
        {
            std::string s = str.substr(i, pos - i);
            result.push_back(s);
            i = pos + static_cast<int>(pattern.size()) - 1;
        }
    }
    return result;
}

std::vector<std::string> DictionariesMetaStoreFDB::splitXml(std::string xml)
{
    int begin = 0;
    std::vector<std::string> dicts;
    std::string pattern_start = "<dictionary>";
    std::string pattern_end = "</dictionary>";
    size_t loc, loc2;
    while (true)
    {
        loc = xml.find(pattern_start, begin);
        loc2 = xml.find(pattern_end, begin);
        if (loc == std::string::npos || loc2 == std::string::npos)
        {
            break;
        }
        loc2 += pattern_end.length();
        begin = static_cast<int>(loc2);
        std::string result = "<dictionaries>\n" + xml.substr(loc, loc2 - loc) + "\n</dictionaries>";
        dicts.push_back(result);
    }
    return dicts;
}

std::string DictionariesMetaStoreFDB::convertConfigToString(Poco::AutoPtr<Poco::Util::AbstractConfiguration> config)
{
    const Poco::Util::XMLConfiguration & xml_config = dynamic_cast<const Poco::Util::XMLConfiguration &>(*config);
    std::stringstream s("");
    std::ostream & str = s;
    xml_config.save(str);
    return s.str();
}

Poco::AutoPtr<Poco::Util::AbstractConfiguration> DictionariesMetaStoreFDB::convertStringToConfig(std::string config_str)
{
    std::stringstream s(config_str);
    std::istream & str = s;
    Poco::AutoPtr<XMLConfiguration> xml_config = new XMLConfiguration(str);
    return xml_config;
}

void DictionariesMetaStoreFDB::createRepositoryInfoFromDictInfo(RepositoryInfo & repo_info, DictInfo dict)
{
    Poco::AutoPtr<Poco::Util::AbstractConfiguration> config = convertStringToConfig(dict.file_contents());
    auto repo = std::make_unique<ExternalLoaderFDBDictionaryConfigRepository>(dict.dict_name(), getContext());
    repo_info.repository = std::move(repo);

    FileInfo file;
    file.file_contents = config;
    file.in_use = true;
    repo_info.files.emplace(dict.dict_name(), file);
}

void DictionariesMetaStoreFDB::createRepositoriesFromDicts(
    std::unordered_map<String, DictInfo> dicts, std::unordered_map<Repository *, RepositoryInfo> & repositories)
{
    for (auto & iter : dicts)
    {
        DictInfo dict = iter.second;
        RepositoryInfo repo_info;
        createRepositoryInfoFromDictInfo(repo_info, dict);
        if (!repo_info.repository)
        {
            LOG_DEBUG(log, "Repository in repo_info is nullptr");
        }
        repositories.emplace(repo_info.repository.get(), RepositoryInfo{std::move(repo_info.repository), repo_info.files});
    }
}
}
