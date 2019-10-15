#pragma once

#include <Poco/Util/AbstractConfiguration.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{
using DictionaryConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

/// Convert dictionary AST to Poco::AbstractConfiguration
/// This function is necessary because all loadable objects configuration are Poco::AbstractConfiguration
DictionaryConfigurationPtr getDictionaryConfigurationFromAST(const ASTCreateQuery & query);

}
