#include <iostream>
#include <sstream>

#include <Poco/File.h>
#include <Poco/FileStream.h>
#include <Poco/AutoPtr.h>
#include <Poco/NumberParser.h>
#include <Poco/String.h>

#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/Element.h>
#include <Poco/DOM/Text.h>
#include <Poco/DOM/NodeList.h>
#include <Poco/DOM/NamedNodeMap.h>
#include <Poco/SAX/InputSource.h>

#include <Common/OptimizedRegularExpression.h>
#include <Common/Exception.h>

#include <Common/UatraitsFast/rules.h>
#include <Common/UatraitsFast/uatraits-fast.h>
#include "Actions.h"
#include "ActionsHelpers.h"


#ifndef LOG_INFO
#define LOG_INFO(logger, message) \
    do { std::cerr << message << std::endl; } while (0)
#endif

#ifndef LOG_WARNING
#define LOG_WARNING(logger, message) \
    do { std::cerr << message << std::endl; } while (0)
#endif

#ifndef LOG_ERROR
#define LOG_ERROR(logger, message) \
    do { std::cerr << message << std::endl; } while (0)
#endif

namespace
{
namespace ErrorCodes
{
extern const int NO_ROOT_ELEMENT;
extern const int NO_ROOT_NODE_ELEMENT;
extern const int UNEXPECTED_NAME_FOR_ROOT_NODE;
extern const int PARSE_NO_VALUE_SPECIFIED;
extern const int PARSE_NO_NAME_SPECIFIED;
extern const int CONDITION_WITHOUT_FIELD;
extern const int CONDITION_WITHOUT_VALUE;
extern const int UNSUPPORTED_CONDITION_TYPE;
extern const int TOO_MANY_CONDITIONS_INSIDE_RULE;
extern const int NO_CONDITIONS_INSIDE_RULE;
}

template <template<typename> class Comparator>
std::shared_ptr<BasicCondition<UATraits::Result>> createCondition(const bool isVersionField, const std::string & field_name, const UATraits::Version version, const StringRef src)
{
    if (isVersionField)
        return std::make_shared<Condition<UATraits::Result, Comparator, UATraits::Version>>(field_name, version);
    else
        return std::make_shared<Condition<UATraits::Result, Comparator, StringRef>>(field_name, src);
};

std::shared_ptr<BasicCondition<UATraits::Result>> parseCondition(Poco::XML::Node * node)
{
    std::shared_ptr<BasicCondition<UATraits::Result>> condition;

    if (node->nodeName() == "group")
    {
        Poco::AutoPtr<Poco::XML::NamedNodeMap> attrs = node->attributes();
        Poco::XML::Node * type = attrs->getNamedItem("type");
        if (!type)
            throw DB::Exception("Condition without type", ErrorCodes::CONDITION_WITHOUT_TYPE);

        std::shared_ptr<BaseGroupCondition<UATraits::Result>> group;

        if (type->getNodeValue() == "and")
        {
            group = std::make_shared<GroupCondition<UATraits::Result, AndType>>();
        }
        else if (type->getNodeValue() == "or")
        {
            group = std::make_shared<GroupCondition<UATraits::Result, OrType>>();
        }
        else
        {
            throw DB::Exception("Unsupported condition type " + type->getNodeValue(), ErrorCodes::UNSUPPORTED_CONDITION_TYPE);
        }
        Poco::AutoPtr<Poco::XML::NodeList> group_children = node->childNodes();

        for (std::size_t i = 0; i < group_children->length(); ++i)
        {
            Poco::XML::Node * group_child = group_children->item(i);

            if (group_child->nodeType() != Poco::XML::Node::ELEMENT_NODE)
                continue;
            group->addCondition(parseCondition(group_child));
        }
        condition = group;
        return condition;
    }
    Poco::AutoPtr<Poco::XML::NamedNodeMap> attrs = node->attributes();
    Poco::XML::Node * field = attrs->getNamedItem("field");
    if (!field)
        throw DB::Exception("Condition without field", ErrorCodes::CONDITION_WITHOUT_FIELD);

    const auto value = node->innerText();
    if (value == "")
        throw DB::Exception("Condition without value", ErrorCodes::CONDITION_WITHOUT_VALUE);

    constexpr std::string_view version_suffix = "Version";

    const auto field_name = field->getNodeValue();

    const auto isVersionField = field_name.size() > version_suffix.size() && std::equal(field_name.begin() + field_name.size() - version_suffix.size(), field_name.end(), version_suffix.begin());

    const StringRef src{value};
    UATraits::Version version;
    if (isVersionField)
    {
        ActionSetVersion::parseVersion(src, version);
    }

    if (node->nodeName() == "eq")
    {
        condition = createCondition<std::equal_to>(isVersionField, field_name, version, src);
    }
    else if (node->nodeName() == "neq")
    {
        condition = createCondition<std::not_equal_to>(isVersionField, field_name, version, src);
    }
    else if (node->nodeName() == "gt")
    {
        condition = createCondition<std::greater>(isVersionField, field_name, version, src);
    }
    else if (node->nodeName() == "lt")
    {
        condition = createCondition<std::less>(isVersionField, field_name, version, src);
    }
    else if (node->nodeName() == "gte")
    {
        condition = createCondition<std::greater_equal>(isVersionField, field_name, version, src);
    }
    else if (node->nodeName() == "lte")
    {
        condition = createCondition<std::less_equal>(isVersionField, field_name, version, src);
    }
    return condition;
}

auto parseRule(Poco::XML::Node * rule)
{
    Poco::AutoPtr<Poco::XML::NamedNodeMap> attrs = rule->attributes();
    Poco::XML::Node * name = attrs->getNamedItem("name");
    if (!name)
        throw DB::Exception("No name specified", ErrorCodes::PARSE_NO_NAME_SPECIFIED);

    Poco::XML::Node * value = attrs->getNamedItem("value");
    if (!value)
        throw DB::Exception("No value specified", ErrorCodes::PARSE_NO_VALUE_SPECIFIED);

    std::shared_ptr<BasicCondition<UATraits::Result>> condition;

    Poco::AutoPtr<Poco::XML::NodeList> rule_children = rule->childNodes();

    for (std::size_t i = 0; i < rule_children->length(); ++i)
    {
        Poco::XML::Node * rule_child = rule_children->item(i);

        if (rule_child->nodeType() != Poco::XML::Node::ELEMENT_NODE)
            continue;

        if (condition)
            throw DB::Exception("Too many conditions inside rule", ErrorCodes::TOO_MANY_CONDITIONS_INSIDE_RULE);

        condition = parseCondition(rule_child);
    }

    if (!condition)
    {
        throw DB::Exception("No conditions inside rule", ErrorCodes::NO_CONDITIONS_INSIDE_RULE);
    }

    return std::make_shared<Rule<UATraits::Result>>(name->getNodeValue(), value->getNodeValue(), condition);
}

}; // namespace

static const size_t MAX_SUBPATTERNS = 5;


std::string UATraits::Version::toString() const
{
    std::stringstream res;

    res << v1 << "." << v2;
    if (v3 || v4)
        res << "." << v3;
    if (v4)
        res << "." << v4;

    return res.str();
}


bool UATraits::Version::empty() const
{
    return v1 == 0 && v2 == 0 && v3 == 0 && v4 == 0;
}


UATraits::Result::Result()
{
}


const UATraits::Result & UATraits::Result::operator =(const UATraits::Result & right)
{
    std::copy(right.string_ref_fields, right.string_ref_fields + StringRefFieldsCount, string_ref_fields);
    std::copy(right.version_fields, right.version_fields + VersionFieldsCount, version_fields);
    std::copy(right.bool_fields, right.bool_fields + BoolFieldsCount, bool_fields);
    std::copy(right.uint_fields, right.uint_fields + UIntFieldsCount, uint_fields);

    user_agent_cache.assign(right.user_agent_cache);
    x_operamini_phone_ua_cache.assign(right.x_operamini_phone_ua_cache);

    detail::FixReferencesHelper::fixReferences(*this, right);

    return *this;
}


void UATraits::Result::dump(std::ostream & ostr) const
{
    bool dumped = false;

     for (size_t i = 0; i < StringRefFieldsCount; ++i)
         if (string_ref_fields[i].size)
         {
             ostr << stringRefFieldNames()[i] << ": ";
             ostr.write(string_ref_fields[i].data, string_ref_fields[i].size);
            ostr << " ";
            dumped  = true;
         }

     for (size_t i = 0; i < VersionFieldsCount; ++i)
         if (!version_fields[i].empty())
        {
             ostr << versionFieldNames()[i] << ": "
                << version_fields[i].toString() << " ";
            dumped  = true;
        }

     for (size_t i = 0; i < UIntFieldsCount; ++i)
         if (uint_fields[i])
        {
            ostr << uIntFieldNames()[i] << ": " << uint_fields[i] << " ";
            dumped  = true;
        }

     for (size_t i = 0; i < BoolFieldsCount; ++i)
         if (bool_fields[i])
        {
            ostr << boolFieldNames()[i] << ": true" << " ";
            dumped  = true;
        }

    if (dumped)
        ostr << std::endl;
}

UATraits::UATraits(const std::string & browser_path, const std::string & profiles_path, const std::string & extra_path)
    : browser_path(browser_path)
    , profiles_path(profiles_path)
    , extra_path(extra_path)
{
    profiles.set_empty_key(StringRef());
    model_to_name.set_empty_key(std::string());
    load();
}

UATraits::UATraits(std::istream & browser_istr, std::istream & profiles_istr, std::istream & extra_istr)
{
    profiles.set_empty_key(StringRef());
    model_to_name.set_empty_key(std::string());
    load(browser_istr, profiles_istr, extra_istr);
}

UATraits::~UATraits()
{};


bool UATraits::load()
{
    if (browser_path.empty() || profiles_path.empty() || extra_path.empty()) {
        return false;
    }
    Poco::File browser_file(browser_path);
    Poco::File profiles_file(profiles_path);
    Poco::File extra_file(extra_path);

    std::time_t browser_new_modification_time = browser_file.getLastModified().epochTime();
    std::time_t profiles_new_modification_time = profiles_file.getLastModified().epochTime();
    std::time_t extra_new_modification_time = extra_file.getLastModified().epochTime();

    if (browser_new_modification_time > browser_modification_time
        || profiles_new_modification_time > profiles_modification_time
        || extra_new_modification_time > extra_modification_time)
    {
        Poco::FileInputStream browser_istr(browser_path);
        Poco::FileInputStream profiles_istr(profiles_path);
        Poco::FileInputStream extra_istr(extra_path);
        load(browser_istr, profiles_istr, extra_istr);

        browser_modification_time = browser_new_modification_time;
        profiles_modification_time = profiles_new_modification_time;
        extra_modification_time = extra_new_modification_time;
        return true;
    }
    return false;
}

void UATraits::load(std::istream & browser_istr, std::istream & profiles_istr, std::istream & extra_istr) {
    strings.clear();
    root_node = Node();
    substrings_count = 0;
    automata_builder = std::make_unique<TDefaultAhoCorasickBuilder>();
    automata.reset();
    substrings_to_indices.clear();
    profiles.clear();

    loadBrowsers(browser_istr);
    loadProfiles(profiles_istr);
    loadExtra(extra_istr);
}


void UATraits::loadProfiles(std::istream & istr)
{
    LOG_INFO(log, "Loading profiles");

    Poco::XML::DOMParser parser;
    Poco::XML::InputSource source(istr);
    Poco::AutoPtr<Poco::XML::Document> document = parser.parse(&source);

    Poco::AutoPtr<Poco::XML::NodeList> root_elem = document->childNodes();

    if (root_elem->length() == 0)
        throw Poco::Exception("No root element in " + std::string(profiles_path));

    Poco::AutoPtr<Poco::XML::NodeList> profiles_elems = root_elem->item(0)->childNodes();

    if (profiles_elems->length() == 0)
        throw Poco::Exception("No profiles found in " + std::string(profiles_path));

    for (size_t i = 0; i < profiles_elems->length(); ++i)
    {
        if (profiles_elems->item(i)->nodeType() != Poco::XML::Node::ELEMENT_NODE)
            continue;

        if (profiles_elems->item(i)->nodeName() != "profile")
        {
            LOG_WARNING(log, "Unknown element in document root: " << profiles_elems->item(i)->nodeName() << ", the only supported is <profile>");
            continue;
        }

        Poco::AutoPtr<Poco::XML::NamedNodeMap> attrs = profiles_elems->item(i)->attributes();
        Poco::XML::Node * url = attrs->getNamedItem("url");
        if (!url)
            throw Poco::Exception("No url specified");

        strings.push_back(url->getNodeValue());
        StringRef url_ref(strings.back().data(), strings.back().size());

        std::pair<Profiles::iterator, bool> inserted = profiles.insert(std::make_pair(url_ref, Actions()));
        if (!inserted.second)
            LOG_WARNING(log, "Duplicate profile " << strings.back());

        Actions & actions = inserted.first->second;

        Poco::AutoPtr<Poco::XML::NodeList> properties = profiles_elems->item(i)->childNodes();
        for (size_t j = 0; j < properties->length(); ++j)
        {
            if (properties->item(j)->nodeType() != Poco::XML::Node::ELEMENT_NODE)
                continue;

            if (properties->item(j)->nodeName() != "define")
            {
                LOG_WARNING(log, "Unknown element in <profile>: " << properties->item(j)->nodeName() << ", the only supported is <define>");
                continue;
            }

            addAction(*properties->item(j), "", actions);
        }
    }

    LOG_INFO(log, "Loaded " << profiles.size() << " profiles");
}


void UATraits::addAction(Poco::XML::Node & define, const std::string & use_name, Actions & actions)
{
    Poco::AutoPtr<Poco::XML::NamedNodeMap> prop_attrs = define.attributes();

    Poco::XML::Node * name = prop_attrs->getNamedItem("name");
    Poco::XML::Node * value = prop_attrs->getNamedItem("value");

    if ((use_name.empty() && !name))
        throw Poco::Exception("No name specified for <define> " + define.innerText());

    if (name && !use_name.empty())
        throw Poco::Exception("Logical error: 'name' attribute exists and 'use_name' parameter specified simultaneously.");

    std::string name_str = !use_name.empty() ? use_name : name->getNodeValue();
    std::string value_or_inner_text = value ? value->getNodeValue() : define.innerText();

    /// compile-time unwinding magic

    if (detail::StringRefFieldAddActionHelper<Strings, Actions>::addAction(strings, actions, name_str, value_or_inner_text)
        || detail::BoolFieldAddActionHelper<Strings, Actions>::addAction(strings, actions, name_str, value_or_inner_text)
        || detail::UIntFieldAddActionHelper<Strings, Actions>::addAction(strings, actions, name_str, value_or_inner_text)
        || detail::VersionFieldAddActionHelper<Strings, Actions>::addAction(strings, actions, name_str, value_or_inner_text)
        || detail::IgnoredFieldAddActionHelper<Strings, Actions>::addAction(strings, actions, name_str, value_or_inner_text))
    {
        return;
    }

    LOG_WARNING(log, "Unknown property: " + name_str);
}


void UATraits::loadBrowsers(std::istream & istr)
{
    LOG_INFO(log, "Loading browsers");

    Poco::XML::DOMParser parser;
    Poco::XML::InputSource source(istr);
    Poco::AutoPtr<Poco::XML::Document> document = parser.parse(&source);

    Poco::AutoPtr<Poco::XML::NodeList> root_elem = document->childNodes();

    if (root_elem->length() == 0)
        throw Poco::Exception("No root element in " + std::string(browser_path));

    auto branch = root_elem->item(0);
    while (branch && branch->nodeType() == Poco::XML::Node::COMMENT_NODE)
        branch = branch->nextSibling();

    if (!branch)
        throw Poco::Exception("No branch element in " + std::string(browser_path));

    addBranch(*branch, root_node);

    /// Доделываем автомат Ахо-Корасик для всех собранных подстрок.
    automata = std::make_unique<const TDefaultMappedAhoCorasick>(automata_builder->Save());
    automata->CheckData();

    LOG_INFO(log, "Loaded browsers");
}


void UATraits::addBranch(Poco::XML::Node & branch, Node & node)
{
    /// Имеется не более одного ребёнка match, а также произвольное количество define и branch.

    Poco::AutoPtr<Poco::XML::NamedNodeMap> attrs = branch.attributes();
    Poco::XML::Node * name = attrs->getNamedItem("name");

    if (name)
        node.name = name->getNodeValue();

    Poco::AutoPtr<Poco::XML::NodeList> branch_children = branch.childNodes();

    for (size_t i = 0; i < branch_children->length(); ++i)
    {
        Poco::XML::Node * branch_child = branch_children->item(i);

        if (branch_child->nodeType() != Poco::XML::Node::ELEMENT_NODE)
            continue;

        if (branch_child->nodeName() == "match")
        {
            processMatch(*branch_child, node);
        }
        else if (branch_child->nodeName() == "define")
        {
            Poco::AutoPtr<Poco::XML::NamedNodeMap> prop_attrs = branch_child->attributes();

            /// Элемент define считается сложным, если есть хотя бы один ребёнок pattern.

            Poco::AutoPtr<Poco::XML::NodeList> pattern_nodes = branch_child->childNodes();
            size_t j;
            for (j = 0; j < pattern_nodes->length(); ++j)
            {
                Poco::XML::Node * pattern_node = pattern_nodes->item(j);
                if (pattern_node->nodeType() == Poco::XML::Node::ELEMENT_NODE && pattern_node->nodeName() == "pattern")
                    break;
            }

            if (j == pattern_nodes->length())
            {
                /// Простой элемент define. Имеет готовое значение value для присваивания. Не содержит внутри себя pattern-ы.
                addAction(*branch_child, "", node.actions);
            }
            else
            {
                /** Каждый вложенный в define элемент pattern обрабатываем, как коллекцию
                  * branch common
                  *  branch exclusive (один или несколько)
                  *   match
                  *    pattern
                  *   define
                  */
                node.children_common.push_back(Node());
                Node & branch_common = node.children_common.back();

                for (size_t j = 0; j < pattern_nodes->length(); ++j)
                {
                    Poco::XML::Node * pattern_node = pattern_nodes->item(j);

                    if (pattern_node->nodeType() != Poco::XML::Node::ELEMENT_NODE)
                        continue;

                    if (pattern_node->nodeName() != "pattern")
                    {
                        LOG_WARNING(log, "Unknown element in <define>: " << pattern_node->nodeName() << ", the only supported is <pattern>");
                        continue;
                    }

                    branch_common.children_exclusive.push_back(Node());
                    Node & surrogate_branch = branch_common.children_exclusive.back();

                    std::string regexp_str;
                    processPattern(*pattern_node, surrogate_branch, regexp_str);

                    if (!regexp_str.empty())
                    {
                        re2::RE2::Options regexp_options;
                        regexp_options.set_case_sensitive(false);

                        surrogate_branch.merged_regexp.reset(new re2::RE2(regexp_str, regexp_options));
                        surrogate_branch.only_one_regexp = true;
                        surrogate_branch.regexp_pattern_num = 0;
                        surrogate_branch.merged_regexp_str = regexp_str;
                        if (!surrogate_branch.merged_regexp->ok())
                            throw Poco::Exception("Cannot compile re2: " + regexp_str + ", error: " + surrogate_branch.merged_regexp->error());

                        surrogate_branch.number_of_subpatterns = surrogate_branch.merged_regexp->NumberOfCapturingGroups();
                        if (surrogate_branch.number_of_subpatterns > MAX_SUBPATTERNS)
                            throw Poco::Exception("Too many subpatterns in regexp: " + regexp_str);
                    }

                    Poco::AutoPtr<Poco::XML::NamedNodeMap> define_attrs = branch_child->attributes();
                    Poco::XML::Node * name = prop_attrs->getNamedItem("name");
                    if (!name)
                        throw Poco::Exception("No 'name' attribute for <define>");

                    addAction(*pattern_node, name->getNodeValue(), surrogate_branch.actions);

                    /// Если name - DeviceName, то заполним соответствие от DeviceModel.
                    if (name->getNodeValue() == "DeviceName")
                    {
                        Poco::AutoPtr<Poco::XML::NamedNodeMap> prop_attrs = pattern_node->attributes();
                        Poco::XML::Node * value = prop_attrs->getNamedItem("value");

                        if (!value)
                            throw Poco::Exception("No 'value' attribute in define for DeviceName.");

                        model_to_name[pattern_node->innerText()] = value->getNodeValue();
                    }
                }
            }
        }
        else if (branch_child->nodeName() == "branch")
        {
            Poco::AutoPtr<Poco::XML::NamedNodeMap> branch_attrs = branch_child->attributes();

            Poco::XML::Node * branch_type = branch_attrs->getNamedItem("type");

            if (!branch_type)
            {
                node.children_exclusive.push_back(Node());
                addBranch(*branch_child, node.children_exclusive.back());
            }
            else if (branch_type->getNodeValue() == "common")
            {
                node.children_common.push_back(Node());
                addBranch(*branch_child, node.children_common.back());
            }
            else if (branch_type->getNodeValue() == "default")
            {
                node.children_default.push_back(Node());
                addBranch(*branch_child, node.children_default.back());
            }
            else
                throw Poco::Exception("Unsupported type '" + branch_type->getNodeValue() + "' of branch, the only supported are 'common', 'default' and no type.");
        }
        else
            LOG_WARNING(log, "Unknown element in <branch> or root node: " << branch_child->nodeName() << ", supported: <match>, <define>, <branch>");
    }
}


void UATraits::processMatch(Poco::XML::Node & match, Node & node)
{
    size_t regexps_count = 0;
    size_t first_regexp_pattern_num = 0;
    std::string first_regexp;
    std::stringstream merged_regexp_stream;
    merged_regexp_stream << "(?:";

    Poco::AutoPtr<Poco::XML::NamedNodeMap> match_attrs = match.attributes();

    Poco::XML::Node * match_type = match_attrs->getNamedItem("type");

    if (match_type && match_type->getNodeValue() != "any")
        throw Poco::Exception("Unsupported type '" + match_type->getNodeValue() + "' of match, the only supported is 'any'");

    Poco::AutoPtr<Poco::XML::NodeList> pattern_nodes = match.childNodes();

    for (size_t j = 0; j < pattern_nodes->length(); ++j)
    {
        Poco::XML::Node * pattern_node = pattern_nodes->item(j);

        if (pattern_node->nodeType() != Poco::XML::Node::ELEMENT_NODE)
            continue;

        if (pattern_node->nodeName() != "pattern")
        {
            LOG_WARNING(log, "Unknown element in <match>: " << pattern_node->nodeName() << ", the only supported is <pattern>");
            continue;
        }

        std::string regexp;
        processPattern(*pattern_node, node, regexp);

        if (!regexp.empty())
        {
            ++regexps_count;

            if (1 == regexps_count)
            {
                first_regexp = regexp;
                first_regexp_pattern_num = node.patterns.size() - 1;
            }
            else
                merged_regexp_stream << "|";

            merged_regexp_stream << regexp;
        }
    }

    merged_regexp_stream << ")";

    if (regexps_count)
    {
        node.merged_regexp_str = regexps_count == 1 ? first_regexp : merged_regexp_stream.str();

        re2::RE2::Options regexp_options;
        regexp_options.set_case_sensitive(false);

        node.merged_regexp.reset(new re2::RE2(node.merged_regexp_str, regexp_options));
        node.only_one_regexp = regexps_count == 1;
        if (node.only_one_regexp)
            node.regexp_pattern_num = first_regexp_pattern_num;

        if (!node.merged_regexp->ok())
            throw Poco::Exception("Cannot compile re2: " + node.merged_regexp_str + ", error: " + node.merged_regexp->error());
    }
}


void UATraits::processPattern(Poco::XML::Node & pattern, Node & node, std::string & regexp)
{
    Poco::AutoPtr<Poco::XML::NamedNodeMap> pattern_attrs = pattern.attributes();

    Poco::XML::Node * pattern_type = pattern_attrs->getNamedItem("type");

    if (!pattern_type)
        throw Poco::Exception("No type specified for <pattern>");

    if (pattern_type->getNodeValue() == "string")
    {
        std::string text = Poco::toLower(pattern.innerText());
        if (substrings_to_indices.end() == substrings_to_indices.find(text))
        {
            const std::string & str = substrings_to_indices.insert(std::make_pair(text, substrings_count)).first->first;

            automata_builder->AddString(std::string{str}, substrings_count);
            node.patterns.push_back(Pattern(substrings_count, false, false));
            node.patterns.back().substring = str;
            ++substrings_count;
        }
        else
        {
            SubstringsToIndices::const_iterator it = substrings_to_indices.find(text);

            node.patterns.push_back(Pattern(it->second, false, false));
            node.patterns.back().substring = it->first;
        }
    }
    else if (pattern_type->getNodeValue() == "regex")
    {
        std::string regexp_str = pattern.innerText();

        /// Исправление для неподдерживаемых assertion-ов
        if (regexp_str == "Windows NT(?!UNTRUSTED)")
            regexp_str = "Windows NT";
        if (regexp_str == "(?<!like )Gecko")
            regexp_str = "(?:[^l]....|l[^i]...|li[^k]..|lik[^e].|like[^ ])Gecko";

        std::string required_substring;
        bool is_trivial = false;
        bool required_substring_is_prefix = false;
        OptimizedRegularExpression::analyze(regexp_str, required_substring, is_trivial, required_substring_is_prefix);

        Poco::toLowerInPlace(required_substring);

        if (!required_substring.empty())
        {
            if (substrings_to_indices.end() == substrings_to_indices.find(required_substring))
            {
                const std::string & str = substrings_to_indices.insert(std::make_pair(required_substring, substrings_count)).first->first;

                automata_builder->AddString(std::string{str}, substrings_count);
                node.patterns.push_back(Pattern(substrings_count, !is_trivial, required_substring_is_prefix));
                node.patterns.back().substring = str;
                if (!is_trivial)
                    node.patterns.back().regexp = regexp_str;
                ++substrings_count;
            }
            else
            {
                SubstringsToIndices::const_iterator it = substrings_to_indices.find(required_substring);

                node.patterns.push_back(Pattern(it->second, !is_trivial, required_substring_is_prefix));
                node.patterns.back().substring = it->first;
                if (!is_trivial)
                    node.patterns.back().regexp = regexp_str;
            }
        }
        else
        {
            node.patterns.push_back(Pattern(Pattern::no_substring, true, false));
            node.patterns.back().regexp = regexp_str;
        }

        if (!is_trivial)
        {
            regexp = regexp_str;
//            std::cerr << regexp << std::endl;
        }
    }
    else
        throw Poco::Exception("Unsupported type '" + pattern.getNodeValue() + "' of pattern, the only supported are 'string' and 'regex'");
}


void UATraits::fixResultAfterOpera(Result & result, const Result & result_by_opera_header)
{
    if (result_by_opera_header.bool_fields[Result::isTablet])
        result.bool_fields[Result::isTablet] = true;

    if (result_by_opera_header.bool_fields[Result::isMobile])
        result.bool_fields[Result::isMobile] = true;

    if (result_by_opera_header.bool_fields[Result::isTouch])
        result.bool_fields[Result::isTouch] = true;

    if (result_by_opera_header.bool_fields[Result::isTV])
        result.bool_fields[Result::isTV] = true;

    /// Рассчитываем на то что здесь не будет возвращаться неизвестная версия ОС.
    if (result_by_opera_header.string_ref_fields[Result::OSName].size)
        result.string_ref_fields[Result::OSName] = result_by_opera_header.string_ref_fields[Result::OSName];

    const StringRef & OSFamily = result_by_opera_header.string_ref_fields[Result::OSFamily];

    if (OSFamily.size && 0 != strncmp(OSFamily.data, "Unknown", strlen("Unknown")))
        result.string_ref_fields[Result::OSFamily] = OSFamily;

    if (!result_by_opera_header.version_fields[Result::OSVersion].empty())
        result.version_fields[Result::OSVersion] = result_by_opera_header.version_fields[Result::OSVersion];

    /// Чтобы не возникало путаницы производителей названий и моделей берём всё из одного места.
    if (result_by_opera_header.string_ref_fields[Result::DeviceVendor].size)
    {
        result.string_ref_fields[Result::DeviceVendor] = result_by_opera_header.string_ref_fields[Result::DeviceVendor];
        result.string_ref_fields[Result::DeviceModel] = result_by_opera_header.string_ref_fields[Result::DeviceModel];
        result.string_ref_fields[Result::DeviceName] = result_by_opera_header.string_ref_fields[Result::DeviceName];
    }

    result.x_operamini_phone_ua_cache = result_by_opera_header.x_operamini_phone_ua_cache;
    detail::FixReferencesHelper::fixReferences(result, result_by_opera_header);
}


void UATraits::detect(
    StringRef user_agent_lower, StringRef profile, StringRef x_operamini_phone_ua_lower,
    Result & result,
    MatchedSubstrings & matched_substrings) const
{
    /// Матчинг User-Agent.

    if (user_agent_lower.size && user_agent_lower.size <= MAX_USER_AGENT_LENGTH)
        detectByUserAgent<SourceUserAgent>(user_agent_lower, result, matched_substrings);

    /// Некоторые свойства могут быть определены из заголовка X-OperaMini-Phone-UA.

    if (x_operamini_phone_ua_lower.size && x_operamini_phone_ua_lower.size < MAX_USER_AGENT_LENGTH)
    {
        Result result_by_opera_header;
        detectByUserAgent<SourceXOperaMiniPhoneUA>(x_operamini_phone_ua_lower, result_by_opera_header, matched_substrings);

        fixResultAfterOpera(result, result_by_opera_header);
    }

    /// Матчинг Profile.

    if (profile.size)
        detectByProfile(profile, result);

    root_rule->trigger(result);
}


void UATraits::detectCaseSafe(
    StringRef user_agent, StringRef user_agent_lower, StringRef profile,
    StringRef x_operamini_phone_ua, StringRef x_operamini_phone_ua_lower,
    Result & result,
    MatchedSubstrings & matched_substrings) const
{
    /// Матчинг User-Agent.

    if (user_agent_lower.size && user_agent_lower.size <= MAX_USER_AGENT_LENGTH)
        detectByUserAgentCaseSafe<SourceUserAgent>(user_agent, user_agent_lower, result, matched_substrings);

    /// Некоторые свойства могут быть определены из заголовка X-OperaMini-Phone-UA.

    if (x_operamini_phone_ua_lower.size && x_operamini_phone_ua_lower.size < MAX_USER_AGENT_LENGTH)
    {
        Result result_by_opera_header;
        detectByUserAgentCaseSafe<SourceXOperaMiniPhoneUA>(x_operamini_phone_ua, x_operamini_phone_ua_lower, result_by_opera_header, matched_substrings);

        fixResultAfterOpera(result, result_by_opera_header);
    }

    /// Матчинг Profile.

    if (profile.size)
        detectByProfile(profile, result);

    root_rule->trigger(result);
}


template <UATraits::SourceType source_type>
void UATraits::detectByUserAgent(const StringRef & user_agent_lower, Result & result, MatchedSubstrings & matched_substrings) const
{
    matched_substrings.clear();
    matched_substrings.resize(substrings_count);

    /// Выполняем автомат Ахо-Корасик для всех собранных подстрок. Байтовая маска найденных подстрок будет записана в matched_substrings;
    const auto search_result = automata->AhoSearch(TStringBuf{user_agent_lower.data, user_agent_lower.size});

    for (const auto & [position, index] : search_result)
    {
        auto & match = matched_substrings[index];
        if (match.positions_len != Match::max_positions)
        {
            ++match.positions_len;
            match.positions[match.positions_len - 1] = position;
        }
    }

    /// Теперь обходим по дереву, и проверяем срабатывание условий.
    traverse(root_node, user_agent_lower, result, matched_substrings);

    /// Сохраним исходную строку, если она используется в полях StringRef.
    detail::CacheSourceHelper<source_type>::cacheSource(result, user_agent_lower);
}


template <UATraits::SourceType source_type>
void UATraits::detectByUserAgentCaseSafe(const StringRef & user_agent, const StringRef & user_agent_lower, Result & result, MatchedSubstrings & matched_substrings) const
{
    matched_substrings.clear();
    matched_substrings.resize(substrings_count);

    /// Выполняем автомат Ахо-Корасик для всех собранных подстрок. Байтовая маска найденных подстрок будет записана в matched_substrings;
    const auto search_result = automata->AhoSearch(TStringBuf{user_agent_lower.data, user_agent_lower.size});

    for (const auto & [position, index] : search_result)
    {
        auto & match = matched_substrings[index];
        if (match.positions_len != Match::max_positions)
        {
            ++match.positions_len;
            match.positions[match.positions_len - 1] = position;
        }
    }


    /// Теперь обходим по дереву, и проверяем срабатывание условий.
    traverse(root_node, user_agent_lower, result, matched_substrings);

    /// Сохраним исходную строку, если она используется в полях StringRef.
    detail::CacheSourceHelper<source_type>::cacheSource(result, user_agent, user_agent_lower);
}


void UATraits::detectByProfile(StringRef profile, Result & result) const
{
    /// Обрезаем двойные кавычки, если они есть.
    if (profile.data[0] == '"')
    {
        ++profile.data;
        --profile.size;
    }

    if (profile.size > 0 && profile.data[profile.size - 1] == '"')
    {
        --profile.size;
    }

    Profiles::const_iterator it = profiles.find(profile);
    if (profiles.end() != it)
    {
        result.bool_fields[Result::isMobile] = true;
        for (size_t i = 0; i < it->second.size(); ++i)
            it->second[i]->execute(StringRef(), result, 0, nullptr);
    }
}


bool UATraits::traverse(const Node & node, StringRef user_agent, Result & result, MatchedSubstrings & matched_substrings) const
{
    //std::cerr << node.name << std::endl;

    bool one_of_regexps_possible = false;
    bool one_of_substrings_matched = false;

    re2::StringPiece matched_subpatterns[MAX_SUBPATTERNS];

    for (size_t i = 0, size = node.patterns.size(); i < size; ++i)
    {
/*        std::cerr << "substring_index: " << node.patterns[i].substring_index << std::endl;
        std::cerr << "is_regexp: " << node.patterns[i].is_regexp << std::endl;
        std::cerr << "substring: " << node.patterns[i].substring << std::endl;
        std::cerr << "regexp: " << node.patterns[i].regexp << std::endl;
        std::cerr << std::endl;
*/
        size_t substring_index = node.patterns[i].substring_index;

        if (substring_index == Pattern::no_substring && node.patterns[i].is_regexp)
        {
            one_of_regexps_possible = true;
        }
        else if (matched_substrings[substring_index].positions_len)
        {
            if (node.patterns[i].is_regexp)
            {
                one_of_regexps_possible = true;
            }
            else
            {
                one_of_substrings_matched = true;
                break;
            }
        }
    }

    bool matched = false;

    if (one_of_substrings_matched)
        matched = true;
    else if (one_of_regexps_possible)
    {
        int start_pos = 0;
        int end_pos = user_agent.size;
        re2::RE2::Anchor anchor = re2::RE2::UNANCHORED;

        if (node.only_one_regexp && node.patterns[node.regexp_pattern_num].required_substring_is_prefix)
        {
            anchor = re2::RE2::ANCHOR_START;

            for (unsigned i = 0; i < matched_substrings[node.patterns[node.regexp_pattern_num].substring_index].positions_len && !matched; ++i)
            {
                if ((start_pos = matched_substrings[node.patterns[node.regexp_pattern_num].substring_index].positions[i] + 1))
                {
                    start_pos -= (node.patterns[node.regexp_pattern_num].substring.size());
                    //std::cerr << "position : " << matched_substrings[node.patterns[node.regexp_pattern_num].substring_index].positions[i] << " substring.size() " << node.patterns[node.regexp_pattern_num].substring.size() <<  std::endl;

                    //std::cerr << "anchored: " << node.merged_regexp_str << ", " << start_pos << std::endl;

                    if (node.number_of_subpatterns)
                    {
                        matched = node.merged_regexp->Match(
                            re2::StringPiece(user_agent.data, user_agent.size),
                            start_pos, end_pos, anchor, matched_subpatterns, node.number_of_subpatterns + 1);
                    }
                    else
                    {
                        matched = node.merged_regexp->Match(
                            re2::StringPiece(user_agent.data, user_agent.size),
                            start_pos, end_pos, anchor, nullptr, 0);
                    }
                }
                else break;
            }
        }
        else
        {
            //std::cerr << "unanchored: " << node.merged_regexp_str << ", " << node.only_one_regexp << ", " << node.patterns[node.regexp_pattern_num].required_substring_is_prefix << std::endl;

            if (node.number_of_subpatterns)
            {
                matched = node.merged_regexp->Match(
                    re2::StringPiece(user_agent.data, user_agent.size),
                    start_pos, end_pos, anchor, matched_subpatterns, node.number_of_subpatterns + 1);
            }
            else
            {
                matched = node.merged_regexp->Match(
                    re2::StringPiece(user_agent.data, user_agent.size),
                    start_pos, end_pos, anchor, nullptr, 0);
            }
        }
    }
    else if (node.patterns.empty())
        matched = true;

    //std::cerr << node.name << ", " << matched << ", " << one_of_regexps_possible << ", " << node.merged_regexp_str << std::endl;

    if (matched)
    {
        for (size_t i = 0, size = node.actions.size(); i < size; ++i)
            node.actions[i]->execute(user_agent, result, node.number_of_subpatterns + 1, matched_subpatterns);

        /// Обрабатываем детей.

        Nodes::const_iterator it;
        for (it = node.children_exclusive.begin(); it != node.children_exclusive.end(); ++it)
            if (traverse(*it, user_agent, result, matched_substrings))
                break;

        if (it == node.children_exclusive.end() && !node.children_default.empty())
            traverse(node.children_default.front(), user_agent, result, matched_substrings);

        for (it = node.children_common.begin(); it != node.children_common.end(); ++it)
            traverse(*it, user_agent, result, matched_substrings);
    }

    return matched;
}


std::string UATraits::getNameByModel(const std::string & model) const
{
    ModelToName::const_iterator it = model_to_name.find(model);

    return it == model_to_name.end()
        ? ""
        : it->second;
}


void UATraits::loadExtra(std::istream & istr)
{
    LOG_INFO(log, "Loading extra");

    Poco::XML::DOMParser parser;
    Poco::XML::InputSource source(istr);
    Poco::AutoPtr<Poco::XML::Document> document = parser.parse(&source);

    Poco::AutoPtr<Poco::XML::NodeList> root_elem = document->childNodes();

    root_rule = std::make_unique<RootRule<Result>>();

    if (root_elem->length() == 0)
        throw DB::Exception("No root element in " + extra_path, ErrorCodes::NO_ROOT_ELEMENT);

    auto root_node = root_elem->item(0);
    while (root_node && root_node->nodeType() == Poco::XML::Node::COMMENT_NODE)
        root_node = root_node->nextSibling();

    if (!root_node)
        throw DB::Exception("No root node element in " + extra_path, ErrorCodes::NO_ROOT_NODE_ELEMENT);

    if (root_node->nodeName() != "rules")
        throw DB::Exception("Unexpected name for root node: " + root_node->nodeName() + ", expected 'rules'", ErrorCodes::UNEXPECTED_NAME_FOR_ROOT_NODE);

    Poco::AutoPtr<Poco::XML::NodeList> branch_children = root_node->childNodes();

    for (size_t i = 0; i < branch_children->length(); ++i)
    {
        Poco::XML::Node * branch_child = branch_children->item(i);

        if (branch_child->nodeType() != Poco::XML::Node::ELEMENT_NODE)
            continue;
        root_rule->addRule(parseRule(branch_child));
    }

    LOG_INFO(log, "Loaded extra");
}
