#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
using DictionaryConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

/// Convert dictionary AST to Poco::AbstractConfiguration
/// This function is necessary because all loadable objects configuration are Poco::AbstractConfiguration
/// Can throw exception if query is ill-formed
DictionaryConfigurationPtr
getDictionaryConfigurationFromAST(const ASTCreateQuery & query, ContextPtr context, const std::string & database_ = "");
}
