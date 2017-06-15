#pragma once

#include <ext/shared_ptr_helper.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/IStorage.h>
#include <common/MultiVersion.h>
#include <common/logger_useful.h>

#include <Interpreters/ExternalDictionaries.h>

namespace DB
{

class StorageDictionary : private ext::shared_ptr_helper<StorageDictionary>, public IStorage
{
    friend class ext::shared_ptr_helper<StorageDictionary>;

public:
    static StoragePtr create(
        const String & table_name_,
        const String & database_name_,
        Context & context_,
        ASTPtr & query_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_);

    std::string getName() const override { return "Dictionary"; }
    std::string getTableName() const override { return table_name; }
    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

    BlockInputStreams read(
        const Names & column_names,
        const ASTPtr& query,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size = DEFAULT_BLOCK_SIZE,
        unsigned threads = 1) override;

    void drop() override {}

private:
    using Ptr = MultiVersion<IDictionaryBase>::Version;

    String select_database_name;
    String select_table_name;
    String table_name;
    String database_name;
    ASTSelectQuery inner_query;
    Context & context;
    NamesAndTypesListPtr columns;
    String dictionary_name;
    const ExternalDictionaries & external_dictionaries;
    Poco::Logger * logger;

    StorageDictionary(
        const String & table_name_,
        const String & database_name_,
        Context & context_,
        ASTPtr & query_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        const ExternalDictionaries & external_dictionaries_);

    void checkNamesAndTypesCompatibleWithDictionary(Ptr dictionary) const;

    NamesAndTypes getNamesAndTypesFromDictionaryStructure(Ptr dictionary) const;

    template <class ForwardIterator>
    std::string generateNamesAndTypesDescription(ForwardIterator begin, ForwardIterator end) const {
        if (begin == end) {
            return "";
        }
        std::string description;
        for (; begin != end; ++begin) {
            description += ", ";
            description += begin->name;
            description += ' ';
            description += begin->type->getName();
        }
        return description.substr(2, description.size());
    }
};

}

