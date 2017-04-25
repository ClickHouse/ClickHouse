
#include <sstream>
#include <Parsers/ASTCreateQuery.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Storages/StorageDictionary.h>
#include <Interpreters/Context.h>

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
    dictionary = external_dictionaries.getDictionary(dictionary_name);
    checkNamesAndTypesCompatibleWithDictionary();
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
    processed_stage = QueryProcessingStage::FetchColumns;
    IDictionarySource* source = const_cast<IDictionarySource*>(dictionary->getSource());
    return BlockInputStreams{source->loadAll()};
}

void StorageDictionary::checkNamesAndTypesCompatibleWithDictionary()
{
    const DictionaryStructure & dictionaryStructure = dictionary->getStructure();
    
    std::set<NameAndTypePair> dictionaryNamesAndTypes;
    for (const auto & attribute : dictionaryStructure.attributes) {
        dictionaryNamesAndTypes.insert(NameAndTypePair(attribute.name, attribute.type));
    }
    if (dictionaryStructure.key) {
        for (const auto & attribute : *dictionaryStructure.key) {
            dictionaryNamesAndTypes.insert(NameAndTypePair(attribute.name, attribute.type));
        }
    }
    
    for (auto & column : *columns) {
        if (dictionaryNamesAndTypes.find(column) == dictionaryNamesAndTypes.end()) {
            std::string message = "Not found column ";
            message += column.name + " " + column.type->getName();
            message += " in dictionary " + dictionary_name + ". ";
            message += "There are only columns ";
            message += generateNamesAndTypesDescription(
                dictionaryNamesAndTypes.begin(), dictionaryNamesAndTypes.end());
            throw Exception(message);
        }
    }
}

}