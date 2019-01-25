#pragma once

#include <Poco/Util/XMLConfiguration.h>
#include <Core/Types.h>
#include <vector>
#include <string>

namespace DB
{

using StringToVector = std::map<String, std::vector<String>>;
using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

void constructSubstitutions(ConfigurationPtr & substitutions_view, StringToVector & out_substitutions);

std::vector<String> formatQueries(const String & query, StringToVector substitutions_to_generate);

}
