#include <Storages/System/StorageSystemDocumentation.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Columns/IColumn.h>
#include <Common/Documentation.h>
#include <Common/FunctionDocumentation.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/DatabaseFactory.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Disks/DiskFactory.h>
#include <Functions/FunctionFactory.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/StorageFactory.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <boost/algorithm/string/trim.hpp>


namespace DB
{

namespace
{

/// The kind of the documented entity. The numeric values are part of the on-the-wire schema and must be kept stable.
enum class EntityType : int8_t
{
    Function = 1,
    AggregateFunction = 2,
    TableFunction = 3,
    TableEngine = 4,
    DatabaseEngine = 5,
    DataType = 6,
    DictionaryLayout = 7,
    DictionarySource = 8,
    AggregateFunctionCombinator = 9,
    DataSkippingIndex = 10,
    DiskType = 11,
};

std::vector<std::pair<String, Int8>> getTypeEnumValues()
{
    return {
        {"Function", static_cast<Int8>(EntityType::Function)},
        {"Aggregate Function", static_cast<Int8>(EntityType::AggregateFunction)},
        {"Table Function", static_cast<Int8>(EntityType::TableFunction)},
        {"Table Engine", static_cast<Int8>(EntityType::TableEngine)},
        {"Database Engine", static_cast<Int8>(EntityType::DatabaseEngine)},
        {"Data Type", static_cast<Int8>(EntityType::DataType)},
        {"Dictionary Layout", static_cast<Int8>(EntityType::DictionaryLayout)},
        {"Dictionary Source", static_cast<Int8>(EntityType::DictionarySource)},
        {"Aggregate Function Combinator", static_cast<Int8>(EntityType::AggregateFunctionCombinator)},
        {"Data Skipping Index", static_cast<Int8>(EntityType::DataSkippingIndex)},
        {"Disk Type", static_cast<Int8>(EntityType::DiskType)},
    };
}

/// Assembles the individual structured parts of an entity's embedded documentation into a single Markdown document,
/// in the same shape as it appears on the website. Empty parts are omitted.
String composeMarkdown(
    const String & description,
    const String & syntax,
    const String & arguments,
    const String & parameters,
    const String & returned_value,
    const String & examples,
    const String & introduced_in,
    const std::vector<String> & related)
{
    String result = boost::algorithm::trim_copy(description);

    auto add_block = [&](std::string_view title, const String & body, bool as_code)
    {
        const String trimmed = boost::algorithm::trim_copy(body);
        if (trimmed.empty())
            return;
        if (!result.empty())
            result += "\n\n";
        result += "**";
        result += title;
        result += "**\n\n";
        if (as_code)
            result += "```sql\n" + trimmed + "\n```";
        else
            result += trimmed;
    };

    add_block("Syntax", syntax, /*as_code=*/ true);
    add_block("Arguments", arguments, /*as_code=*/ false);
    add_block("Parameters", parameters, /*as_code=*/ false);
    add_block("Returned value", returned_value, /*as_code=*/ false);
    add_block("Examples", examples, /*as_code=*/ false);

    const String introduced = boost::algorithm::trim_copy(introduced_in);
    if (!introduced.empty())
    {
        if (!result.empty())
            result += "\n\n";
        result += "**Introduced in:** " + introduced;
    }

    if (!related.empty())
    {
        String related_str;
        for (const auto & name : related)
        {
            if (!related_str.empty())
                related_str += ", ";
            related_str += "`" + name + "`";
        }
        if (!result.empty())
            result += "\n\n";
        result += "**Related:** " + related_str;
    }

    return result;
}

String renderDoc(const Documentation & doc)
{
    return composeMarkdown(
        doc.description,
        doc.syntaxAsString(),
        /*arguments=*/ "",
        /*parameters=*/ "",
        /*returned_value=*/ "",
        doc.examplesAsString(),
        doc.introducedInAsString(),
        doc.related);
}

String renderFunctionDoc(const FunctionDocumentation & doc)
{
    return composeMarkdown(
        doc.description,
        doc.syntaxAsString(),
        doc.argumentsAsString(),
        doc.parametersAsString(),
        doc.returnedValueAsString(),
        doc.examplesAsString(),
        doc.introducedInAsString(),
        /*related=*/ {});
}

void addRow(MutableColumns & res_columns, EntityType type, const String & name, const String & description)
{
    res_columns[0]->insert(name);
    res_columns[1]->insert(static_cast<Int8>(type));
    res_columns[2]->insert(description);
}

/// For function-like factories (regular and aggregate functions) which carry `FunctionDocumentation` and have aliases.
template <typename Factory>
void addFunctionLike(MutableColumns & res_columns, EntityType type, const Factory & factory)
{
    for (const auto & name : factory.getAllRegisteredNames())
    {
        if (factory.isAlias(name))
            addRow(res_columns, type, name, "Alias of `" + factory.aliasTo(name) + "`.");
        else
            addRow(res_columns, type, name, renderFunctionDoc(factory.getDocumentation(name)));
    }
}

/// For factories which carry `Documentation` and have no aliases.
template <typename Factory>
void addDocumented(MutableColumns & res_columns, EntityType type, const Factory & factory)
{
    for (const auto & name : factory.getAllRegisteredNames())
        addRow(res_columns, type, name, renderDoc(factory.getDocumentation(name)));
}

/// For factories which carry `Documentation` and have aliases (data type families).
template <typename Factory>
void addDocumentedWithAliases(MutableColumns & res_columns, EntityType type, const Factory & factory)
{
    for (const auto & name : factory.getAllRegisteredNames())
    {
        if (factory.isAlias(name))
            addRow(res_columns, type, name, "Alias of `" + factory.aliasTo(name) + "`.");
        else
            addRow(res_columns, type, name, renderDoc(factory.getDocumentation(name)));
    }
}

}

ColumnsDescription StorageSystemDocumentation::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the entity, e.g. `domainWithoutWWW` or `MergeTree`."},
        {"type", std::make_shared<DataTypeEnum8>(getTypeEnumValues()), "The kind of the entity, e.g. `Function` or `Table Engine`."},
        {"description", std::make_shared<DataTypeString>(),
            "The reference documentation of the entity rendered as Markdown, assembled from the embedded documentation "
            "(the same content as published on the website), including syntax, examples and other structured parts, if any."},
    };
}

void StorageSystemDocumentation::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    addFunctionLike(res_columns, EntityType::Function, FunctionFactory::instance());
    addFunctionLike(res_columns, EntityType::AggregateFunction, AggregateFunctionFactory::instance());

    {
        const auto & factory = TableFunctionFactory::instance();
        for (const auto & name : factory.getAllRegisteredNames())
        {
            String description;
            if (auto documentation = factory.tryGetDocumentation(name))
                description = renderFunctionDoc(*documentation);
            addRow(res_columns, EntityType::TableFunction, name, description);
        }
    }

    for (const auto & [name, creator] : StorageFactory::instance().getAllStorages())
        addRow(res_columns, EntityType::TableEngine, name, renderDoc(creator.documentation));

    for (const auto & [name, creator] : DatabaseFactory::instance().getDatabaseEngines())
        addRow(res_columns, EntityType::DatabaseEngine, name, renderDoc(creator.documentation));

    addDocumentedWithAliases(res_columns, EntityType::DataType, DataTypeFactory::instance());
    addDocumented(res_columns, EntityType::DictionaryLayout, DictionaryFactory::instance());
    addDocumented(res_columns, EntityType::DictionarySource, DictionarySourceFactory::instance());

    for (const auto & combinator : AggregateFunctionCombinatorFactory::instance().getAllAggregateFunctionCombinators())
    {
        if (combinator.combinator_ptr->isForInternalUsageOnly())
            continue;
        addRow(res_columns, EntityType::AggregateFunctionCombinator, combinator.name, renderDoc(combinator.documentation));
    }

    addDocumented(res_columns, EntityType::DataSkippingIndex, MergeTreeIndexFactory::instance());
    addDocumented(res_columns, EntityType::DiskType, DiskFactory::instance());
}

}
