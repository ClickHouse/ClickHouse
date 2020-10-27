#include <Storages/StorageDictionary.h>
#include <Storages/StorageFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Parsers/ASTLiteral.h>
#include <Common/quoteString.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>
#include <sstream>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int THERE_IS_NO_COLUMN;
    extern const int CANNOT_DETACH_DICTIONARY_AS_TABLE;
}

namespace
{
    void checkNamesAndTypesCompatibleWithDictionary(const String & dictionary_name, const ColumnsDescription & columns, const DictionaryStructure & dictionary_structure)
    {
        auto dictionary_names_and_types = StorageDictionary::getNamesAndTypes(dictionary_structure);
        std::set<NameAndTypePair> names_and_types_set(dictionary_names_and_types.begin(), dictionary_names_and_types.end());

        for (const auto & column : columns.getOrdinary())
        {
            if (names_and_types_set.find(column) == names_and_types_set.end())
            {
                std::string message = "Not found column ";
                message += column.name + " " + column.type->getName();
                message += " in dictionary " + backQuote(dictionary_name) + ". ";
                message += "There are only columns ";
                message += StorageDictionary::generateNamesAndTypesDescription(dictionary_names_and_types);
                throw Exception(message, ErrorCodes::THERE_IS_NO_COLUMN);
            }
        }
    }
}


NamesAndTypesList StorageDictionary::getNamesAndTypes(const DictionaryStructure & dictionary_structure)
{
    NamesAndTypesList dictionary_names_and_types;

    if (dictionary_structure.id)
        dictionary_names_and_types.emplace_back(dictionary_structure.id->name, std::make_shared<DataTypeUInt64>());

    /// In old-style (XML) configuration we don't have this attributes in the
    /// main attribute list, so we have to add them to columns list explicitly.
    /// In the new configuration (DDL) we have them both in range_* nodes and
    /// main attribute list, but for compatibility we add them before main
    /// attributes list.
    if (dictionary_structure.range_min)
        dictionary_names_and_types.emplace_back(dictionary_structure.range_min->name, dictionary_structure.range_min->type);

    if (dictionary_structure.range_max)
        dictionary_names_and_types.emplace_back(dictionary_structure.range_max->name, dictionary_structure.range_max->type);

    if (dictionary_structure.key)
    {
        for (const auto & attribute : *dictionary_structure.key)
            dictionary_names_and_types.emplace_back(attribute.name, attribute.type);
    }

    for (const auto & attribute : dictionary_structure.attributes)
    {
        /// Some attributes can be already added (range_min and range_max)
        if (!dictionary_names_and_types.contains(attribute.name))
            dictionary_names_and_types.emplace_back(attribute.name, attribute.type);
    }

    return dictionary_names_and_types;
}


String StorageDictionary::generateNamesAndTypesDescription(const NamesAndTypesList & list)
{
    std::stringstream ss;
    bool first = true;
    for (const auto & name_and_type : list)
    {
        if (!std::exchange(first, false))
            ss << ", ";
        ss << name_and_type.name << ' ' << name_and_type.type->getName();
    }
    return ss.str();
}


StorageDictionary::StorageDictionary(
    const StorageID & table_id_,
    const String & dictionary_name_,
    const ColumnsDescription & columns_,
    Location location_)
    : IStorage(table_id_)
    , dictionary_name(dictionary_name_)
    , location(location_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}


StorageDictionary::StorageDictionary(
    const StorageID & table_id_, const String & dictionary_name_, const DictionaryStructure & dictionary_structure_, Location location_)
    : StorageDictionary(table_id_, dictionary_name_, ColumnsDescription{getNamesAndTypes(dictionary_structure_)}, location_)
{
}


void StorageDictionary::checkTableCanBeDropped() const
{
    if (location == Location::SameDatabaseAndNameAsDictionary)
        throw Exception("Cannot detach dictionary " + backQuote(dictionary_name) + " as table, use DETACH DICTIONARY query", ErrorCodes::CANNOT_DETACH_DICTIONARY_AS_TABLE);
    if (location == Location::DictionaryDatabase)
        throw Exception("Cannot detach table " + getStorageID().getFullTableName() + " from a database with DICTIONARY engine", ErrorCodes::CANNOT_DETACH_DICTIONARY_AS_TABLE);
}

Pipes StorageDictionary::read(
    const Names & column_names,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const unsigned /*threads*/)
{
    auto dictionary = context.getExternalDictionariesLoader().getDictionary(dictionary_name);
    auto stream = dictionary->getBlockInputStream(column_names, max_block_size);
    auto source = std::make_shared<SourceFromInputStream>(stream);
    /// TODO: update dictionary interface for processors.
    Pipes pipes;
    pipes.emplace_back(std::move(source));
    return pipes;
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

        if (!args.attach)
        {
            const auto & dictionary = args.context.getExternalDictionariesLoader().getDictionary(dictionary_name);
            const DictionaryStructure & dictionary_structure = dictionary->getStructure();
            checkNamesAndTypesCompatibleWithDictionary(dictionary_name, args.columns, dictionary_structure);
        }

        return StorageDictionary::create(args.table_id, dictionary_name, args.columns, StorageDictionary::Location::Custom);
    });
}

}
