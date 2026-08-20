#pragma once

#include <Core/QualifiedTableName.h>
#include <Interpreters/Context_fwd.h>

#include <Poco/AutoPtr.h>

namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}
}

namespace DB
{

class ASTCreateQuery;
using DictionaryConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

/// Convert dictionary AST to Poco::AbstractConfiguration
/// This function is necessary because all loadable objects configuration are Poco::AbstractConfiguration
/// Can throw exception if query is ill-formed
DictionaryConfigurationPtr
getDictionaryConfigurationFromAST(const ASTCreateQuery & query, ContextPtr context, const std::string & database_ = "");

struct ClickHouseDictionarySourceInfo
{
    QualifiedTableName table_name;
    String query;
    bool is_local = false;
};

std::optional<ClickHouseDictionarySourceInfo>
getInfoIfClickHouseDictionarySource(DictionaryConfigurationPtr & config, ContextPtr global_context);

}
