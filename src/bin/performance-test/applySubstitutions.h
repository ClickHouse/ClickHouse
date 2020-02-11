#pragma once

#include <Poco/Util/XMLConfiguration.h>
#include <Core/Types.h>
#include <vector>
#include <string>
#include <map>

namespace DB
{

using StringToVector = std::map<std::string, Strings>;
using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

void constructSubstitutions(ConfigurationPtr & substitutions_view, StringToVector & out_substitutions);

Strings formatQueries(const std::string & query, StringToVector substitutions_to_generate);

}
