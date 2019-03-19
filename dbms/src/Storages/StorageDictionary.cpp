#include <sstream>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExternalDictionaries.h>
#include <Parsers/ASTLiteral.h>
#include <common/logger_useful.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int THERE_IS_NO_COLUMN;
}


StorageDictionary::StorageDictionary(
    const String & table_name_,
    const ColumnsDescription & columns_,
    const Context & context,
    bool attach,
    const String & dictionary_name_)
    : IStorage{columns_}, table_name(table_name_),
    dictionary_name(dictionary_name_),
    logger(&Poco::Logger::get("StorageDictionary"))
{
    if (!attach)
    {
        const auto & dictionary = context.getExternalDictionaries().getDictionary(dictionary_name);
        const DictionaryStructure & dictionary_structure = dictionary->getStructure();
        checkNamesAndTypesCompatibleWithDictionary(dictionary_structure);
    }
}

BlockInputStreams StorageDictionary::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const unsigned /*threads*/)
{
    auto dictionary = context.getExternalDictionaries().getDictionary(dictionary_name);
    return BlockInputStreams{dictionary->getBlockInputStream(column_names, max_block_size)};
}

NamesAndTypesList StorageDictionary::getNamesAndTypes(const DictionaryStructure & dictionary_structure)
{
    NamesAndTypesList dictionary_names_and_types;

    if (dictionary_structure.id)
        dictionary_names_and_types.emplace_back(dictionary_structure.id->name, std::make_shared<DataTypeUInt64>());
    if (dictionary_structure.range_min)
        dictionary_names_and_types.emplace_back(dictionary_structure.range_min->name, dictionary_structure.range_min->type);
    if (dictionary_structure.range_max)
        dictionary_names_and_types.emplace_back(dictionary_structure.range_max->name, dictionary_structure.range_max->type);
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
    std::set<NameAndTypePair> names_and_types_set(dictionary_names_and_types.begin(), dictionary_names_and_types.end());

    for (const auto & column : getColumns().getOrdinary())
    {
        if (names_and_types_set.find(column) == names_and_types_set.end())
        {
            std::string message = "Not found column ";
            message += column.name + " " + column.type->getName();
            message += " in dictionary " + dictionary_name + ". ";
            message += "There are only columns ";
            message += generateNamesAndTypesDescription(dictionary_names_and_types.begin(), dictionary_names_and_types.end());
            throw Exception(message, ErrorCodes::THERE_IS_NO_COLUMN);
        }
    }
}

void registerStorageDictionary(StorageFactory & factory)
{
    factory.registerStorage("Dictionary", [](const StorageFactory::Arguments & args)
    {
        if (args.engine_args.size() != 1)
            throw Exception("Storage Dictionary requires single parameter: name of dictionary",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        args.engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args.engine_args[0], args.local_context);
        String dictionary_name = args.engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

        return StorageDictionary::create(
            args.table_name, args.columns, args.context, args.attach, dictionary_name);
    });
}

}
