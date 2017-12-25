#include <sstream>
#include <Parsers/ASTCreateQuery.h>
#include <DataTypes/DataTypesNumber.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/CacheDictionary.h>
#include <Storages/StorageDictionary.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionaries.h>
#include <common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB
{

StoragePtr StorageDictionary::create(
    const String & table_name,
    Context & context,
    const ASTCreateQuery & query,
    const NamesAndTypes & columns,
    const NamesAndTypes & materialized_columns,
    const NamesAndTypes & alias_columns,
    const ColumnDefaults & column_defaults)
{
    const ASTFunction & engine = *query.storage->engine;
    String dictionary_name;
    if (engine.arguments)
    {
        std::stringstream iss;
        engine.arguments->format(IAST::FormatSettings(iss, false, false));
        dictionary_name = iss.str();
    }

    const auto & dictionary = context.getExternalDictionaries().getDictionary(dictionary_name);
    const DictionaryStructure & dictionary_structure = dictionary->getStructure();
    return ext::shared_ptr_helper<StorageDictionary>::create(
        table_name, columns, materialized_columns, alias_columns,
        column_defaults, dictionary_structure, dictionary_name);
}

StoragePtr StorageDictionary::create(
    const String & table_name,
    const NamesAndTypes & columns,
    const NamesAndTypes & materialized_columns,
    const NamesAndTypes & alias_columns,
    const ColumnDefaults & column_defaults,
    const DictionaryStructure & dictionary_structure,
    const String & dictionary_name)
{
    return ext::shared_ptr_helper<StorageDictionary>::create(
        table_name, columns, materialized_columns, alias_columns,
        column_defaults, dictionary_structure, dictionary_name);
}

StorageDictionary::StorageDictionary(
    const String & table_name_,
    const NamesAndTypes & columns_,
    const NamesAndTypes & materialized_columns_,
    const NamesAndTypes & alias_columns_,
    const ColumnDefaults & column_defaults_,
    const DictionaryStructure & dictionary_structure_,
    const String & dictionary_name_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_}, table_name(table_name_),
    columns(columns_), dictionary_name(dictionary_name_),
    logger(&Poco::Logger::get("StorageDictionary"))
{
    checkNamesAndTypesCompatibleWithDictionary(dictionary_structure_);
}

BlockInputStreams StorageDictionary::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned /*threads*/)
{
    processed_stage = QueryProcessingStage::FetchColumns;
    auto dictionary = context.getExternalDictionaries().getDictionary(dictionary_name);
    return BlockInputStreams{dictionary->getBlockInputStream(column_names, max_block_size)};
}

NamesAndTypes StorageDictionary::getNamesAndTypes(const DictionaryStructure & dictionary_structure)
{
    NamesAndTypes dictionary_names_and_types;

    if (dictionary_structure.id)
        dictionary_names_and_types.emplace_back(dictionary_structure.id->name, std::make_shared<DataTypeUInt64>());
    if (dictionary_structure.range_min)
        dictionary_names_and_types.emplace_back(dictionary_structure.range_min->name, std::make_shared<DataTypeUInt16>());
    if (dictionary_structure.range_max)
        dictionary_names_and_types.emplace_back(dictionary_structure.range_max->name, std::make_shared<DataTypeUInt16>());
    if (dictionary_structure.key)
        for (const auto & attribute : *dictionary_structure.key)
            dictionary_names_and_types.emplace_back(attribute.name, attribute.type);

    for (const auto & attribute : dictionary_structure.attributes)
        dictionary_names_and_types.emplace_back(attribute.name, attribute.type);

    return dictionary_names_and_types;
}

void StorageDictionary::checkNamesAndTypesCompatibleWithDictionary(const DictionaryStructure & dictionary_structure) const
{
    auto dictionary_names_and_types = getNamesAndTypes(dictionary_structure);
    std::set<NameAndType> namesAndTypesSet(dictionary_names_and_types.begin(), dictionary_names_and_types.end());

    for (auto & column : columns)
    {
        if (namesAndTypesSet.find(column) == namesAndTypesSet.end())
        {
            std::string message = "Not found column ";
            message += column.name + " " + column.type->getName();
            message += " in dictionary " + dictionary_name + ". ";
            message += "There are only columns ";
            message += generateNamesAndTypesDescription(dictionary_names_and_types.begin(), dictionary_names_and_types.end());
            throw Exception(message);
        }
    }
}

}
