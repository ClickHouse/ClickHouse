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
    ASTPtr & query,
    NamesAndTypesListPtr columns,
    const NamesAndTypesList & materialized_columns,
    const NamesAndTypesList & alias_columns,
    const ColumnDefaults & column_defaults)
{
    ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query);
    const ASTFunction & function = typeid_cast<const ASTFunction &> (*create.storage);
    String dictionary_name;
    if (function.arguments)
    {
        std::stringstream iss;
        function.arguments->format(IAST::FormatSettings(iss, false, false));
        dictionary_name = iss.str();
    }

    const auto & dictionary = context.getExternalDictionaries().getDictionary(dictionary_name);
    const DictionaryStructure & dictionary_structure = dictionary->getStructure();
    return make_shared(table_name, columns, materialized_columns, alias_columns,
                       column_defaults, dictionary_structure, dictionary_name);
}

StoragePtr StorageDictionary::create(
    const String & table_name,
    NamesAndTypesListPtr columns,
    const NamesAndTypesList & materialized_columns,
    const NamesAndTypesList & alias_columns,
    const ColumnDefaults & column_defaults,
    const DictionaryStructure & dictionary_structure,
    const String & dictionary_name)
{
    return make_shared(table_name, columns, materialized_columns, alias_columns,
                       column_defaults, dictionary_structure, dictionary_name);
}

StorageDictionary::StorageDictionary(
    const String & table_name_,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
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
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned threads)
{
    processed_stage = QueryProcessingStage::FetchColumns;
    auto dictionary = context.getExternalDictionaries().getDictionary(dictionary_name);
    return BlockInputStreams{dictionary->getBlockInputStream(column_names, max_block_size)};
}

NamesAndTypesListPtr StorageDictionary::getNamesAndTypes(const DictionaryStructure & dictionaryStructure)
{
    NamesAndTypesListPtr dictionaryNamesAndTypes = std::make_shared<NamesAndTypesList>();

    if (dictionaryStructure.id)
        dictionaryNamesAndTypes->push_back(NameAndTypePair(dictionaryStructure.id->name,
                                                          std::make_shared<DataTypeUInt64>()));
    if (dictionaryStructure.range_min)
        dictionaryNamesAndTypes->push_back(NameAndTypePair(dictionaryStructure.range_min->name,
                                                          std::make_shared<DataTypeUInt16>()));
    if (dictionaryStructure.range_max)
        dictionaryNamesAndTypes->push_back(NameAndTypePair(dictionaryStructure.range_max->name,
                                                          std::make_shared<DataTypeUInt16>()));
    if (dictionaryStructure.key)
        for (const auto & attribute : *dictionaryStructure.key)
            dictionaryNamesAndTypes->push_back(NameAndTypePair(attribute.name, attribute.type));

    for (const auto & attribute : dictionaryStructure.attributes)
        dictionaryNamesAndTypes->push_back(NameAndTypePair(attribute.name, attribute.type));

    return dictionaryNamesAndTypes;
}

void StorageDictionary::checkNamesAndTypesCompatibleWithDictionary(const DictionaryStructure & dictionaryStructure) const
{
    auto dictionaryNamesAndTypes = getNamesAndTypes(dictionaryStructure);
    std::set<NameAndTypePair> namesAndTypesSet(dictionaryNamesAndTypes->begin(), dictionaryNamesAndTypes->end());

    for (auto & column : *columns)
    {
        if (namesAndTypesSet.find(column) == namesAndTypesSet.end())
        {
            std::string message = "Not found column ";
            message += column.name + " " + column.type->getName();
            message += " in dictionary " + dictionary_name + ". ";
            message += "There are only columns ";
            message += generateNamesAndTypesDescription(dictionaryNamesAndTypes->begin(), dictionaryNamesAndTypes->end());
            throw Exception(message);
        }
    }
}

}
