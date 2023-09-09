#include <Interpreters/FunctionsMetaStoreFDB.h>
#include <Common/FoundationDB/MetadataStoreFoundationDB.h>
#include <Common/FoundationDB/protos/MetadataFuctions.pb.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <IO/WriteBufferFromString.h>
#include <cstdint>
#include <memory>
#include <Interpreters/ExternalLoaderXMLConfigRepository.h>
#include <s2/base/integral_types.h>
#include <Poco/Util/XMLConfiguration.h>
#include <sstream>
#include <string>
#include <vector>
#include <ctime>
namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
FunctionsMetaStoreFDB::FunctionsMetaStoreFDB(ContextPtr context_)
    : WithContext(context_), meta_store(context_->getMetadataStoreFoundationDB()), log(&Poco::Logger::get("FunctionMetaStoreFDB"))
{
}

Poco::Timestamp FunctionsMetaStoreFDB::getUpdateTime(const std::string & func_name)
{
    const std::lock_guard<std::mutex> lock(meta_store_mutex);
    time_t update_time = static_cast<time_t>(meta_store->getOneFunction(func_name)->update_time());
    return Poco::Timestamp::fromEpochTime(update_time);
}

bool FunctionsMetaStoreFDB::addOneFunc(
    ExternalLoaderXMLConfigRepository * repository, const std::unordered_map<Repository *, RepositoryInfo> & repositories)
{
    const std::lock_guard<std::mutex> lock(meta_store_mutex);

    if (repository == nullptr)
        return false;
    std::unordered_map<std::string /* path */, FileInfo> files = repositories.at(repository).files;
    for (auto & iter : files)
    {
        FileInfo file = iter.second;
        std::string file_contents = convertConfigToString(file.file_contents);
        /// Splitting string by '<function>' to get all functions.
        std::vector<std::string> funcs_xml = splitXML(file_contents);
        for (auto xml : funcs_xml)
        {
            auto new_config = convertStringToConfig(xml);
            std::string func_name = new_config->getString("function.name");
            if (meta_store->existOneFunction(func_name))
            {
                continue;
            }
            XmlFuncInfo func;
            func.set_xml_udf_name(func_name);
            func.set_file_contents(xml);
            func.set_name(func_name);
            uint64 now = static_cast<uint64>(time(nullptr));
            func.set_update_time(now);
            meta_store->addOneFunction(func, func_name);
        }
    }
    return true;
}

bool FunctionsMetaStoreFDB::deleteOneFunc(const std::string & func_name)
{
    const std::lock_guard<std::mutex> lock(meta_store_mutex);
    return meta_store->deleteOneFunction(func_name);
}

bool FunctionsMetaStoreFDB::exist(const std::string & func_name) const
{
    return meta_store->existOneFunction(func_name);
}

RepositoryInfo FunctionsMetaStoreFDB::getOneFunc(const std::string & func_name)
{
    const std::lock_guard<std::mutex> lock(meta_store_mutex);
    std::unique_ptr<XmlFuncInfo> func = meta_store->getOneFunction(func_name);
    RepositoryInfo repository_info;
    createRepositoryInfoFromFuncInfo(repository_info, *func);
    return repository_info;
}

DB::LoadablesConfigurationPtr FunctionsMetaStoreFDB::getOneFuncConfig(const std::string & func_name)
{
    const std::lock_guard<std::mutex> lock(meta_store_mutex);
    std::unique_ptr<XmlFuncInfo> fdb_func = meta_store->getOneFunction(func_name);
    return convertStringToConfig(fdb_func->file_contents());
}

std::vector<std::string> FunctionsMetaStoreFDB::list()
{
    const std::lock_guard<std::mutex> lock(meta_store_mutex);
    std::vector<std::unique_ptr<XmlFuncInfo>> functions = meta_store->getAllFunctions();
    std::vector<std::string> function_names;
    function_names.reserve(functions.size());
    for (auto & function : functions)
    {
        function_names.push_back(function->xml_udf_name());
    }
    return function_names;
}
/// Convert the repositories from fdb into map
std::vector<std::unique_ptr<Repository>> FunctionsMetaStoreFDB::getAllFunc()
{
    const std::lock_guard<std::mutex> lock(meta_store_mutex);
    std::vector<std::unique_ptr<Repository>> repos;
    std::vector<std::unique_ptr<XmlFuncInfo>> fdb_funcs = meta_store->getAllFunctions();
    for (auto & fdb_func : fdb_funcs)
    {
        auto repo = std::make_unique<ExternalLoaderFDBFunctionConfigRepository>(fdb_func->name(), getContext());
        repos.push_back(std::move(repo));
    }
    return repos;
}

bool FunctionsMetaStoreFDB::isFirstOpen()
{
    const std::lock_guard<std::mutex> lock(meta_store_mutex);
    return meta_store->isFirstBoot();
}


/// Tools to split string.
std::vector<std::string> FunctionsMetaStoreFDB::split(std::string & str, std::string & pattern)
{
    int pos;
    std::vector<std::string> result;
    str += pattern;
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

std::vector<std::string> FunctionsMetaStoreFDB::splitXML(std::string & xml)
{
    int begin = 0;
    std::vector<std::string> funcs;
    std::string pattern_start = "<function>";
    std::string pattern_end = "</function>";
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
        std::string result = "<functions>\n" + xml.substr(loc, loc2 - loc) + "\n</functions>";
        funcs.push_back(result);
    }
    return funcs;
}

std::string FunctionsMetaStoreFDB::convertConfigToString(Poco::AutoPtr<Poco::Util::AbstractConfiguration> config)
{
    const Poco::Util::XMLConfiguration & xml_config = dynamic_cast<const Poco::Util::XMLConfiguration &>(*config);
    std::stringstream s("");
    std::ostream & str = s;
    xml_config.save(str);
    return s.str();
}

Poco::AutoPtr<Poco::Util::AbstractConfiguration> FunctionsMetaStoreFDB::convertStringToConfig(std::string config_str)
{
    std::stringstream s(config_str);
    std::istream & str = s;
    Poco::AutoPtr<XMLConfiguration> xml_config = new XMLConfiguration(str);
    return xml_config;
}

void FunctionsMetaStoreFDB::createRepositoryInfoFromFuncInfo(RepositoryInfo & repo_info, XmlFuncInfo func)
{
    Poco::AutoPtr<Poco::Util::AbstractConfiguration> config = convertStringToConfig(func.file_contents());
    auto repo = std::make_unique<ExternalLoaderFDBFunctionConfigRepository>(func.xml_udf_name(), getContext());
    repo_info.repository = std::move(repo);

    FileInfo file;
    file.file_contents = config;
    repo_info.files.emplace(func.xml_udf_name(), file);
}

void FunctionsMetaStoreFDB::createRepositoriesFromFuncs(
    std::unordered_map<String, XmlFuncInfo> funcs, std::unordered_map<Repository *, RepositoryInfo> & repositories)
{
    for (auto & iter : funcs)
    {
        XmlFuncInfo func = iter.second;
        RepositoryInfo repo_info;
        createRepositoryInfoFromFuncInfo(repo_info, func);
        if (!repo_info.repository)
        {
            LOG_DEBUG(log, "Repository in repo_info is nullptr");
        }
        repositories.emplace(repo_info.repository.get(), RepositoryInfo{std::move(repo_info.repository), repo_info.files});
    }
}
}
