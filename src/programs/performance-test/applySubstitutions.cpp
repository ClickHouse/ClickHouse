#include "applySubstitutions.h"
#include <algorithm>
#include <vector>

namespace DB
{

void constructSubstitutions(ConfigurationPtr & substitutions_view, StringToVector & out_substitutions)
{
    Strings xml_substitutions;
    substitutions_view->keys(xml_substitutions);

    for (size_t i = 0; i != xml_substitutions.size(); ++i)
    {
        const ConfigurationPtr xml_substitution(substitutions_view->createView("substitution[" + std::to_string(i) + "]"));

        /// Property values for substitution will be stored in a vector
        /// accessible by property name
        Strings xml_values;
        xml_substitution->keys("values", xml_values);

        std::string name = xml_substitution->getString("name");

        for (size_t j = 0; j != xml_values.size(); ++j)
        {
            out_substitutions[name].push_back(xml_substitution->getString("values.value[" + std::to_string(j) + "]"));
        }
    }
}

/// Recursive method which goes through all substitution blocks in xml
/// and replaces property {names} by their values
static void runThroughAllOptionsAndPush(StringToVector::iterator substitutions_left,
    StringToVector::iterator substitutions_right,
    const std::string & template_query,
    Strings & out_queries)
{
    if (substitutions_left == substitutions_right)
    {
        out_queries.push_back(template_query); /// completely substituted query
        return;
    }

    std::string substitution_mask = "{" + substitutions_left->first + "}";

    if (template_query.find(substitution_mask) == std::string::npos) /// nothing to substitute here
    {
        runThroughAllOptionsAndPush(std::next(substitutions_left), substitutions_right, template_query, out_queries);
        return;
    }

    for (const std::string & value : substitutions_left->second)
    {
        /// Copy query string for each unique permutation
        std::string query = template_query;
        size_t substr_pos = 0;

        while (substr_pos != std::string::npos)
        {
            substr_pos = query.find(substitution_mask);

            if (substr_pos != std::string::npos)
                query.replace(substr_pos, substitution_mask.length(), value);
        }

        runThroughAllOptionsAndPush(std::next(substitutions_left), substitutions_right, query, out_queries);
    }
}

Strings formatQueries(const std::string & query, StringToVector substitutions_to_generate)
{
    Strings queries_res;
    runThroughAllOptionsAndPush(
        substitutions_to_generate.begin(),
        substitutions_to_generate.end(),
        query,
        queries_res);
    return queries_res;
}


}
