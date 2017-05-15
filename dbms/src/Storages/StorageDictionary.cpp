
#include <sstream>
#include <Parsers/ASTCreateQuery.h>
#include <DataTypes/DataTypesNumber.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/CacheDictionary.h>
#include <Storages/StorageDictionary.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>

namespace DB {

StoragePtr StorageDictionary::create(
    const String & table_name_,
    const String & database_name_,
    Context & context_,
    ASTPtr & query_,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_)
{
    return make_shared(
        table_name_, database_name_, context_, query_,
        columns_, materialized_columns_, alias_columns_, column_defaults_,
        context_.getExternalDictionaries()
    );
}

StorageDictionary::StorageDictionary(
    const String & table_name_,
    const String & database_name_,
    Context & context_,
    ASTPtr & query_,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    const ExternalDictionaries & external_dictionaries_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_}, table_name(table_name_),
    database_name(database_name_), context(context_), columns(columns_),
    external_dictionaries(external_dictionaries_)
{
    logger = &Poco::Logger::get("StorageDictionary");
    ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*query_);
    const ASTFunction & function = typeid_cast<const ASTFunction &> (*create.storage);
    if (function.arguments) {
        std::stringstream iss;
        function.arguments->format(IAST::FormatSettings(iss, false, false));
        dictionary_name = iss.str();
    }
    auto dictionary = external_dictionaries.getDictionary(dictionary_name);
    checkNamesAndTypesCompatibleWithDictionary(dictionary);
}

BlockInputStreams StorageDictionary::read(
    const Names & column_names,
    ASTPtr query,
    const Context & context,
    const Settings & settings,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned threads)
{
    processed_stage = QueryProcessingStage::Complete;
    auto dictionary = external_dictionaries.getDictionary(dictionary_name);
    return BlockInputStreams{dictionary->getBlockInputStream(column_names)};
}

NamesAndTypes StorageDictionary::getNamesAndTypesFromDictionaryStructure(Ptr dictionary) const
{
    const DictionaryStructure & dictionaryStructure = dictionary->getStructure();

    NamesAndTypes dictionaryNamesAndTypes;

    if (dictionaryStructure.id)
        dictionaryNamesAndTypes.push_back(NameAndTypePair(dictionaryStructure.id->name,
                                                          std::make_shared<DataTypeUInt64>()));
    if (dictionaryStructure.range_min)
        dictionaryNamesAndTypes.push_back(NameAndTypePair(dictionaryStructure.range_min->name,
                                                          std::make_shared<DataTypeUInt16>()));
    if (dictionaryStructure.range_max)
        dictionaryNamesAndTypes.push_back(NameAndTypePair(dictionaryStructure.range_max->name,
                                                          std::make_shared<DataTypeUInt16>()));
    if (dictionaryStructure.key)
        for (const auto & attribute : *dictionaryStructure.key)
            dictionaryNamesAndTypes.push_back(NameAndTypePair(attribute.name, attribute.type));

    for (const auto & attribute : dictionaryStructure.attributes)
        dictionaryNamesAndTypes.push_back(NameAndTypePair(attribute.name, attribute.type));

    return dictionaryNamesAndTypes;
}

void StorageDictionary::checkNamesAndTypesCompatibleWithDictionary(Ptr dictionary) const
{
    auto dictionaryNamesAndTypes = getNamesAndTypesFromDictionaryStructure(dictionary);
    std::set<NameAndTypePair> namesAndTypesSet(dictionaryNamesAndTypes.begin(), dictionaryNamesAndTypes.end());

    for (auto & column : *columns) {
        if (namesAndTypesSet.find(column) == namesAndTypesSet.end()) {
            std::string message = "Not found column ";
            message += column.name + " " + column.type->getName();
            message += " in dictionary " + dictionary_name + ". ";
            message += "There are only columns ";
            message += generateNamesAndTypesDescription(dictionaryNamesAndTypes.begin(), dictionaryNamesAndTypes.end());
            throw Exception(message);
        }
    }
}

}
