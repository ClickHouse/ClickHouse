#include <Dictionaries/getDictionaryConfigurationFromAST.h>

#include <Poco/DOM/AutoPtr.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/Element.h>
#include <Poco/DOM/Text.h>
#include <Poco/Net/NetException.h>
#include <Poco/Util/XMLConfiguration.h>
#include <IO/WriteHelpers.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Core/Names.h>
#include <Common/FieldVisitorToString.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Functions/FunctionFactory.h>
#include <Common/isLocalAddress.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DICTIONARY_DEFINITION;
}


/// There are a lot of code, but it's very simple and straightforward
/// We just perform conversion.
namespace
{

struct AttributeConfiguration
{
    std::string type;
    std::string expression;
};

using AttributeNameToConfiguration = std::unordered_map<std::string, AttributeConfiguration>;

String getAttributeExpression(const ASTDictionaryAttributeDeclaration * dict_attr)
{
    if (!dict_attr->expression)
        return {};

    /// EXPRESSION PROPERTY should be expression or string
    String expression_str;
    if (const auto * literal = dict_attr->expression->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
        expression_str = convertFieldToString(literal->value);
    else
        expression_str = queryToString(dict_attr->expression);

    return expression_str;
}


using namespace Poco;
using namespace Poco::XML;

/* Transforms next definition
 *  LIFETIME(MIN 10, MAX 100)
 * to the next configuration
 *  <lifetime>
 *    <min>10</min>
 *    <max>100</max>
 *  </lifetime>
 */
void buildLifetimeConfiguration(
    AutoPtr<Document> doc,
    AutoPtr<Element> root,
    const ASTDictionaryLifetime * lifetime)
{
    if (!lifetime)
        return;

    AutoPtr<Element> lifetime_element(doc->createElement("lifetime"));
    AutoPtr<Element> min_element(doc->createElement("min"));
    AutoPtr<Element> max_element(doc->createElement("max"));
    AutoPtr<Text> min_sec(doc->createTextNode(toString(lifetime->min_sec)));
    min_element->appendChild(min_sec);
    AutoPtr<Text> max_sec(doc->createTextNode(toString(lifetime->max_sec)));
    max_element->appendChild(max_sec);
    lifetime_element->appendChild(min_element);
    lifetime_element->appendChild(max_element);
    root->appendChild(lifetime_element);
}

/* Transforms next definition
 *  LAYOUT(FLAT())
 * to the next configuration
 *  <layout>
 *    <flat/>
 *  </layout>
 *
 * And next definition
 *  LAYOUT(CACHE(SIZE_IN_CELLS 1000))
 * to the next one
 *  <layout>
 *    <cache>
 *      <size_in_cells>1000</size_in_cells>
 *    </cache>
 *  </layout>
 */
void buildLayoutConfiguration(
    AutoPtr<Document> doc,
    AutoPtr<Element> root,
    const ASTDictionarySettings * settings,
    const ASTDictionaryLayout * layout)
{
    AutoPtr<Element> layout_element(doc->createElement("layout"));
    root->appendChild(layout_element);
    AutoPtr<Element> layout_type_element(doc->createElement(layout->layout_type));
    layout_element->appendChild(layout_type_element);

    if (!layout->parameters)
        return;

    if (settings != nullptr)
    {
        AutoPtr<Element> settings_element(doc->createElement("settings"));
        root->appendChild(settings_element);
        for (const auto & [name, value] : settings->changes)
        {
            AutoPtr<Element> setting_change_element(doc->createElement(name));
            settings_element->appendChild(setting_change_element);
            AutoPtr<Text> setting_value(doc->createTextNode(convertFieldToString(value)));
            setting_change_element->appendChild(setting_value);
        }
    }

    for (const auto & param : layout->parameters->children)
    {
        const ASTPair * pair = param->as<ASTPair>();
        if (!pair)
        {
            throw DB::Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Dictionary layout parameters must be key/value pairs, got '{}' instead",
                param->formatForErrorMessage());
        }

        const ASTLiteral * value_literal = pair->second->as<ASTLiteral>();
        if (!value_literal)
        {
            throw DB::Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Dictionary layout parameter value must be a literal, got '{}' instead",
                pair->second->formatForErrorMessage());
        }

        const auto value_field = value_literal->value;

        if (value_field.getType() != Field::Types::UInt64 && value_field.getType() != Field::Types::Float64 && value_field.getType() != Field::Types::String)
        {
            throw DB::Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Dictionary layout parameter value must be an UInt64, Float64 or String, got '{}' instead",
                value_field.getTypeName());
        }

        AutoPtr<Element> layout_type_parameter_element(doc->createElement(pair->first));
        AutoPtr<Text> value_to_append(doc->createTextNode(toString(value_field)));
        layout_type_parameter_element->appendChild(value_to_append);
        layout_type_element->appendChild(layout_type_parameter_element);
    }
}


/* Transforms next definition
 *  RANGE(MIN StartDate, MAX EndDate)
 * to the next configuration
 *  <range_min><name>StartDate</name></range_min>
 *  <range_max><name>EndDate</name></range_max>
 */
void buildRangeConfiguration(AutoPtr<Document> doc, AutoPtr<Element> root, const ASTDictionaryRange * range, const AttributeNameToConfiguration & all_attrs)
{
    // appends <key><name>value</name></key> to root
    auto append_element = [&doc, &root](const std::string & key, const std::string & name, const AttributeConfiguration & configuration)
    {
        AutoPtr<Element> element(doc->createElement(key));
        AutoPtr<Element> name_node(doc->createElement("name"));
        AutoPtr<Text> name_text(doc->createTextNode(name));
        name_node->appendChild(name_text);
        element->appendChild(name_node);

        AutoPtr<Element> type_node(doc->createElement("type"));
        AutoPtr<Text> type_text(doc->createTextNode(configuration.type));
        type_node->appendChild(type_text);
        element->appendChild(type_node);

        if (!configuration.expression.empty())
        {
            AutoPtr<Element> expression_node(doc->createElement("expression"));
            AutoPtr<Text> expression_text(doc->createTextNode(configuration.expression));
            expression_node->appendChild(expression_text);
            element->appendChild(expression_node);
        }

        root->appendChild(element);
    };

    auto range_min_attribute_it = all_attrs.find(range->min_attr_name);
    if (range_min_attribute_it == all_attrs.end())
        throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION,
            "MIN {} attribute is not defined in the dictionary attributes", range->min_attr_name);

    auto range_max_attribute_it = all_attrs.find(range->min_attr_name);
    if (range_max_attribute_it == all_attrs.end())
        throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION,
            "MAX {} attribute is not defined in the dictionary attributes", range->max_attr_name);

    append_element("range_min", range->min_attr_name, range_min_attribute_it->second);
    append_element("range_max", range->max_attr_name, range_max_attribute_it->second);
}


/// Get primary key columns names from AST
Names getPrimaryKeyColumns(const ASTExpressionList * primary_key)
{
    Names result;
    const auto & children = primary_key->children;

    for (const auto & child : children)
    {
        const ASTIdentifier * key_part = child->as<const ASTIdentifier>();
        result.push_back(key_part->name());
    }
    return result;
}

void buildAttributeExpressionIfNeeded(
    AutoPtr<Document> doc,
    AutoPtr<Element> root,
    const ASTDictionaryAttributeDeclaration * dict_attr)
{
    if (!dict_attr->expression)
        return;

    AutoPtr<Element> expression_element(doc->createElement("expression"));
    String expression_str = getAttributeExpression(dict_attr);
    AutoPtr<Text> expression(doc->createTextNode(expression_str));
    expression_element->appendChild(expression);
    root->appendChild(expression_element);
}

/** Transforms single dictionary attribute to configuration
  *  third_column UInt8 DEFAULT 2 EXPRESSION rand() % 100 * 77
  * to
  *  <attribute>
  *      <name>third_column</name>
  *      <type>UInt8</type>
  *      <null_value>2</null_value>
  *      <expression>(rand() % 100) * 77</expression>
  *  </attribute>
  */
void buildSingleAttribute(
    AutoPtr<Document> doc,
    AutoPtr<Element> root,
    const ASTDictionaryAttributeDeclaration * dict_attr)
{
    AutoPtr<Element> attribute_element(doc->createElement("attribute"));
    root->appendChild(attribute_element);

    AutoPtr<Element> name_element(doc->createElement("name"));
    AutoPtr<Text> name(doc->createTextNode(dict_attr->name));
    name_element->appendChild(name);
    attribute_element->appendChild(name_element);

    AutoPtr<Element> type_element(doc->createElement("type"));
    AutoPtr<Text> type(doc->createTextNode(queryToString(dict_attr->type)));
    type_element->appendChild(type);
    attribute_element->appendChild(type_element);

    AutoPtr<Element> null_value_element(doc->createElement("null_value"));
    String null_value_str;
    if (dict_attr->default_value)
        null_value_str = convertFieldToString(dict_attr->default_value->as<ASTLiteral>()->value);
    AutoPtr<Text> null_value(doc->createTextNode(null_value_str));
    null_value_element->appendChild(null_value);
    attribute_element->appendChild(null_value_element);

    buildAttributeExpressionIfNeeded(doc, attribute_element, dict_attr);

    if (dict_attr->hierarchical)
    {
        AutoPtr<Element> hierarchical_element(doc->createElement("hierarchical"));
        AutoPtr<Text> hierarchical(doc->createTextNode("true"));
        hierarchical_element->appendChild(hierarchical);
        attribute_element->appendChild(hierarchical_element);
    }

    if (dict_attr->bidirectional)
    {
        AutoPtr<Element> bidirectional_element(doc->createElement("bidirectional"));
        AutoPtr<Text> bidirectional(doc->createTextNode("true"));
        bidirectional_element->appendChild(bidirectional);
        attribute_element->appendChild(bidirectional_element);
    }

    if (dict_attr->injective)
    {
        AutoPtr<Element> injective_element(doc->createElement("injective"));
        AutoPtr<Text> injective(doc->createTextNode("true"));
        injective_element->appendChild(injective);
        attribute_element->appendChild(injective_element);
    }

    if (dict_attr->is_object_id)
    {
        AutoPtr<Element> is_object_id_element(doc->createElement("is_object_id"));
        AutoPtr<Text> is_object_id(doc->createTextNode("true"));
        is_object_id_element->appendChild(is_object_id);
        attribute_element->appendChild(is_object_id_element);
    }
}


/** Transforms
  *   PRIMARY KEY Attr1, ..., AttrN
  * to the next configuration
  *  <id><name>Attr1</name></id>
  * or
  *  <key>
  *    <attribute>
  *        <name>Attr1</name>
  *        <type>UInt8</type>
  *    </attribute>
  *    ...
  *    <attribute> fe
  *  </key>
  *
  */
void buildPrimaryKeyConfiguration(
    AutoPtr<Document> doc,
    AutoPtr<Element> root,
    bool complex,
    const Names & key_names,
    const ASTExpressionList * dictionary_attributes)
{
    const auto & children = dictionary_attributes->children;

    if (!complex)
    {
        if (key_names.size() != 1)
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION,
                "Primary key for simple dictionary must contain exactly one element");

        AutoPtr<Element> id_element(doc->createElement("id"));
        root->appendChild(id_element);
        AutoPtr<Element> name_element(doc->createElement("name"));
        id_element->appendChild(name_element);

        auto identifier_name = key_names.front();

        const auto * it = std::find_if(
            children.begin(),
            children.end(),
            [&](const ASTPtr & node)
            {
                const ASTDictionaryAttributeDeclaration * dict_attr = node->as<const ASTDictionaryAttributeDeclaration>();
                return dict_attr->name == identifier_name;
            });

        if (it == children.end())
        {
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION,
                "Primary key field '{}' not found among attributes",
                identifier_name);
        }

        const ASTDictionaryAttributeDeclaration * dict_attr = (*it)->as<const ASTDictionaryAttributeDeclaration>();

        AutoPtr<Text> name(doc->createTextNode(dict_attr->name));
        name_element->appendChild(name);

        buildAttributeExpressionIfNeeded(doc, id_element, dict_attr);

        if (!dict_attr->type)
            return;

        AutoPtr<Element> type_element(doc->createElement("type"));
        id_element->appendChild(type_element);

        AutoPtr<Text> type(doc->createTextNode(queryToString(dict_attr->type)));
        type_element->appendChild(type);
    }
    else
    {
        if (children.size() < key_names.size())
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION,
                "Primary key fields count is more, than dictionary attributes count.");

        AutoPtr<Element> key_element(doc->createElement("key"));
        root->appendChild(key_element);
        for (const auto & key_name : key_names)
        {
            bool found = false;
            for (const auto & attr : children)
            {
                const ASTDictionaryAttributeDeclaration * dict_attr = attr->as<const ASTDictionaryAttributeDeclaration>();
                if (dict_attr->name == key_name)
                {
                    found = true;
                    buildSingleAttribute(doc, key_element, dict_attr);
                    break;
                }
            }
            if (!found)
                throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION,
                    "Primary key field '{}' not found among attributes.",
                    key_name);
        }
    }
}


/** Transforms list of ASTDictionaryAttributeDeclarations to list of dictionary attributes
  */
AttributeNameToConfiguration buildDictionaryAttributesConfiguration(
    AutoPtr<Document> doc,
    AutoPtr<Element> root,
    const ASTExpressionList * dictionary_attributes,
    const Names & key_columns)
{
    const auto & children = dictionary_attributes->children;
    AttributeNameToConfiguration attributes_name_to_configuration;

    for (const auto & child : children)
    {
        const ASTDictionaryAttributeDeclaration * dict_attr = child->as<const ASTDictionaryAttributeDeclaration>();
        if (!dict_attr->type)
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "Dictionary attribute must has type");

        AttributeConfiguration attribute_configuration {queryToString(dict_attr->type), getAttributeExpression(dict_attr)};
        attributes_name_to_configuration.emplace(dict_attr->name, std::move(attribute_configuration));

        if (std::find(key_columns.begin(), key_columns.end(), dict_attr->name) == key_columns.end())
            buildSingleAttribute(doc, root, dict_attr);
    }
    return attributes_name_to_configuration;
}

/** Transform function with key-value arguments to configuration
  * (used for source transformation)
  */
void buildConfigurationFromFunctionWithKeyValueArguments(
    AutoPtr<Document> doc,
    AutoPtr<Element> root,
    const ASTExpressionList * ast_expr_list,
    ContextPtr context)
{
    const auto & children = ast_expr_list->children;
    for (const auto & child : children)
    {
        const ASTPair * pair = child->as<const ASTPair>();
        AutoPtr<Element> current_xml_element(doc->createElement(pair->first));
        root->appendChild(current_xml_element);

        if (const auto * identifier = pair->second->as<const ASTIdentifier>())
        {
            AutoPtr<Text> value(doc->createTextNode(identifier->name()));
            current_xml_element->appendChild(value);
        }
        else if (const auto * literal = pair->second->as<const ASTLiteral>())
        {
            AutoPtr<Text> value(doc->createTextNode(convertFieldToString(literal->value)));
            current_xml_element->appendChild(value);
        }
        else if (const auto * list = pair->second->as<const ASTExpressionList>())
        {
            buildConfigurationFromFunctionWithKeyValueArguments(doc, current_xml_element, list, context);
        }
        else if (const auto * func = pair->second->as<ASTFunction>())
        {
            /// This branch exists only for compatibility.
            /// It's not possible to have a function in a dictionary definition since 22.10,
            /// because query must be normalized on dictionary creation. It's possible only when we load old metadata.
            /// For debug builds allow it only during server startup to avoid crash in BC check in Stress Tests.
            assert(Context::getGlobalContextInstance()->getApplicationType() != Context::ApplicationType::SERVER
                   || !Context::getGlobalContextInstance()->isServerCompletelyStarted());
            auto builder = FunctionFactory::instance().tryGet(func->name, context);
            auto function = builder->build({});
            function->prepare({});

            /// We assume that function will not take arguments and will return constant value like tcpPort or hostName
            /// Such functions will return column with size equal to input_rows_count.
            size_t input_rows_count = 1;
            auto result = function->execute({}, function->getResultType(), input_rows_count);

            Field value;
            result->get(0, value);

            AutoPtr<Text> text_value(doc->createTextNode(convertFieldToString(value)));
            current_xml_element->appendChild(text_value);
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION,
                "Incorrect ASTPair contains wrong value, should be literal, identifier or list");
        }
    }
}

/** Build source definition from ast.
  *   SOURCE(MYSQL(HOST 'localhost' PORT 9000 USER 'default' REPLICA(HOST '127.0.0.1' PRIORITY 1) PASSWORD ''))
  * to
  *   <source>
  *       <mysql>
  *           <host>localhost</host>
  *           ...
  *           <replica>
  *               <host>127.0.0.1</host>
  *               ...
  *           </replica>
  *       </mysql>
  *   </source>
  */
void buildSourceConfiguration(
    AutoPtr<Document> doc,
    AutoPtr<Element> root,
    const ASTFunctionWithKeyValueArguments * source,
    const ASTDictionarySettings * settings,
    const String & dictionary_name,
    ContextPtr context)
{
    DictionarySourceFactory::instance().checkSourceAvailable(source->name, dictionary_name, context);

    AutoPtr<Element> outer_element(doc->createElement("source"));
    root->appendChild(outer_element);
    AutoPtr<Element> source_element(doc->createElement(source->name));
    outer_element->appendChild(source_element);
    buildConfigurationFromFunctionWithKeyValueArguments(doc, source_element, source->elements->as<const ASTExpressionList>(), context);

    if (settings != nullptr)
    {
        AutoPtr<Element> settings_element(doc->createElement("settings"));
        outer_element->appendChild(settings_element);
        for (const auto & [name, value] : settings->changes)
        {
            AutoPtr<Element> setting_change_element(doc->createElement(name));
            settings_element->appendChild(setting_change_element);
            AutoPtr<Text> setting_value(doc->createTextNode(convertFieldToString(value)));
            setting_change_element->appendChild(setting_value);
        }
    }
}

/** Check all AST fields are filled, throws exception
  * in other case
  */
void checkAST(const ASTCreateQuery & query)
{
    if (!query.is_dictionary || query.dictionary == nullptr)
        throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "Cannot convert dictionary to configuration from non-dictionary AST.");

    if (query.dictionary->layout == nullptr)
        throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "Cannot create dictionary with empty layout");

    const auto is_direct_layout = !strcasecmp(query.dictionary->layout->layout_type.data(), "direct") ||
                                !strcasecmp(query.dictionary->layout->layout_type.data(), "complex_key_direct");
    if (query.dictionary->lifetime == nullptr && !is_direct_layout)
        throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "Cannot create dictionary with empty lifetime");

    if (query.dictionary->primary_key == nullptr)
        throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "Cannot create dictionary without primary key");

    if (query.dictionary->source == nullptr)
        throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "Cannot create dictionary with empty source");
}

void checkPrimaryKey(const AttributeNameToConfiguration & all_attrs, const Names & key_attrs)
{
    for (const auto & key_attr : key_attrs)
        if (all_attrs.find(key_attr) == all_attrs.end())
            throw Exception(ErrorCodes::INCORRECT_DICTIONARY_DEFINITION, "Unknown key attribute '{}'", key_attr);
}

}

void checkLifetime(const ASTCreateQuery & query)
{
    if (query.dictionary->layout && query.dictionary->layout->layout_type == "direct")
    {
        if (query.dictionary->lifetime)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "'lifetime' parameter is redundant for the dictionary' of layout '{}'",
                query.dictionary->layout->layout_type);
    }
}


DictionaryConfigurationPtr
getDictionaryConfigurationFromAST(const ASTCreateQuery & query, ContextPtr context, const std::string & database_)
{
    checkAST(query);
    checkLifetime(query);

    String dictionary_name = query.getTable();
    String db_name = !database_.empty() ? database_ : query.getDatabase();
    String full_dictionary_name = (!db_name.empty() ? (db_name + ".") : "") + dictionary_name;

    AutoPtr<Poco::XML::Document> xml_document(new Poco::XML::Document());
    AutoPtr<Poco::XML::Element> document_root(xml_document->createElement("dictionaries"));
    xml_document->appendChild(document_root);
    AutoPtr<Poco::XML::Element> current_dictionary(xml_document->createElement("dictionary"));
    document_root->appendChild(current_dictionary);
    AutoPtr<Poco::Util::XMLConfiguration> conf(new Poco::Util::XMLConfiguration());

    AutoPtr<Poco::XML::Element> name_element(xml_document->createElement("name"));
    current_dictionary->appendChild(name_element);
    AutoPtr<Text> name(xml_document->createTextNode(dictionary_name));
    name_element->appendChild(name);

    AutoPtr<Poco::XML::Element> database_element(xml_document->createElement("database"));
    current_dictionary->appendChild(database_element);
    AutoPtr<Text> database(xml_document->createTextNode(db_name));
    database_element->appendChild(database);

    if (query.uuid != UUIDHelpers::Nil)
    {
        AutoPtr<Poco::XML::Element> uuid_element(xml_document->createElement("uuid"));
        current_dictionary->appendChild(uuid_element);
        AutoPtr<Text> uuid(xml_document->createTextNode(toString(query.uuid)));
        uuid_element->appendChild(uuid);
    }

    AutoPtr<Element> structure_element(xml_document->createElement("structure"));
    current_dictionary->appendChild(structure_element);
    Names pk_attrs = getPrimaryKeyColumns(query.dictionary->primary_key);
    auto * dictionary_layout = query.dictionary->layout;

    bool complex = DictionaryFactory::instance().isComplex(dictionary_layout->layout_type);

    auto all_attr_names_and_types = buildDictionaryAttributesConfiguration(
        xml_document, structure_element, query.dictionary_attributes_list, pk_attrs);

    checkPrimaryKey(all_attr_names_and_types, pk_attrs);

    /// If the pk size is 1 and pk's DataType is not number, we should convert to complex.
    /// NOTE: the data type of Numeric key(simple layout) is UInt64, so if the type is not under UInt64, type casting will lead to precision loss.
    DataTypePtr first_key_type = DataTypeFactory::instance().get(all_attr_names_and_types.find(pk_attrs[0])->second.type);
    if ((pk_attrs.size() > 1 || (pk_attrs.size() == 1 && !isNumber(first_key_type)))
        && !complex
        && DictionaryFactory::instance().convertToComplex(dictionary_layout->layout_type))
    {
        complex = true;
    }

    buildPrimaryKeyConfiguration(xml_document, structure_element, complex, pk_attrs, query.dictionary_attributes_list);

    buildLayoutConfiguration(xml_document, current_dictionary, query.dictionary->dict_settings, dictionary_layout);
    buildSourceConfiguration(xml_document, current_dictionary, query.dictionary->source, query.dictionary->dict_settings, full_dictionary_name, context);
    buildLifetimeConfiguration(xml_document, current_dictionary, query.dictionary->lifetime);

    if (query.dictionary->range)
        buildRangeConfiguration(xml_document, structure_element, query.dictionary->range, all_attr_names_and_types);

    if (query.comment)
    {
        AutoPtr<Element> comment_element(xml_document->createElement("comment"));
        current_dictionary->appendChild(comment_element);
        AutoPtr<Text> comment_value(xml_document->createTextNode(query.comment->as<ASTLiteral>()->value.safeGet<String>()));

        comment_element->appendChild(comment_value);
    }

    conf->load(xml_document);
    return conf;
}

std::optional<ClickHouseDictionarySourceInfo>
getInfoIfClickHouseDictionarySource(DictionaryConfigurationPtr & config, ContextPtr global_context)
{
    ClickHouseDictionarySourceInfo info;

    bool secure = config->getBool("dictionary.source.clickhouse.secure", false);
    UInt16 default_port = secure ? global_context->getTCPPortSecure().value_or(0) : global_context->getTCPPort();

    String host = config->getString("dictionary.source.clickhouse.host", "localhost");
    UInt16 port = config->getUInt("dictionary.source.clickhouse.port", default_port);
    String database = config->getString("dictionary.source.clickhouse.db", "");
    String table = config->getString("dictionary.source.clickhouse.table", "");

    info.query = config->getString("dictionary.source.clickhouse.query", "");

    if (!table.empty())
        info.table_name = {database, table};
    else if (info.query.empty())
        return {};

    try
    {
        if (isLocalAddress({host, port}, default_port))
            info.is_local = true;
    }
    catch (const Poco::Net::DNSException &)
    {
        /// Server may fail to start if we cannot resolve some hostname. It's ok to ignore exception and leave is_local false.
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return info;
}

}
