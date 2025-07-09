#include <Common/FileRenamer.h>

#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/re2.h>

#include <chrono>
#include <filesystem>
#include <format>
#include <map>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

FileRenamer::FileRenamer() = default;

FileRenamer::FileRenamer(const String & renaming_rule)
    : rule(renaming_rule)
{
    FileRenamer::validateRenamingRule(rule, true);
}

String FileRenamer::generateNewFilename(const String & filename) const
{
    // Split filename and extension
    String file_base = fs::path(filename).stem();
    String file_ext = fs::path(filename).extension();

    // Get current timestamp in microseconds
    String timestamp;
    if (rule.find("%t") != String::npos)
    {
        auto now = std::chrono::system_clock::now();
        timestamp = std::to_string(timeInMicroseconds(now));
    }

    // Define placeholders and their corresponding values
    std::map<String, String> placeholders =
    {
        {"%a", filename},
        {"%f", file_base},
        {"%e", file_ext},
        {"%t", timestamp},
        {"%%", "%"}
    };

    // Replace placeholders with their actual values
    String new_name = rule;
    for (const auto & [placeholder, value] : placeholders)
        boost::replace_all(new_name, placeholder, value);

    return new_name;
}

bool FileRenamer::isEmpty() const
{
    return rule.empty();
}

bool FileRenamer::validateRenamingRule(const String & rule, bool throw_on_error)
{
    // Check if the rule contains invalid placeholders
    re2::RE2 invalid_placeholder_pattern("^([^%]|%[afet%])*$");
    if (!re2::RE2::FullMatch(rule, invalid_placeholder_pattern))
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid renaming rule: Allowed placeholders only %a, %f, %e, %t, and %%");
        return false;
    }

    // Replace valid placeholders with empty strings and count remaining percentage signs.
    String replaced_rule = rule;
    boost::replace_all(replaced_rule, "%a", "");
    boost::replace_all(replaced_rule, "%f", "");
    boost::replace_all(replaced_rule, "%e", "");
    boost::replace_all(replaced_rule, "%t", "");
    if (std::count(replaced_rule.begin(), replaced_rule.end(), '%') % 2)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid renaming rule: Odd number of consecutive percentage signs");
        return false;
    }

    return true;
}


} // DB
