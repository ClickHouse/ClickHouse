#pragma once

#include <Storages/IStorage.h>
#include <Core/Defines.h>
#include <common/MultiVersion.h>
#include <ext/shared_ptr_helper.h>


namespace Poco
{
class Logger;
}

namespace DB
{
struct DictionaryStructure;
struct IDictionaryBase;
class ExternalDictionaries;

class StorageDictionary : public ext::shared_ptr_helper<StorageDictionary>, public IStorage
{
public:
    std::string getName() const override { return "Dictionary"; }
    std::string getTableName() const override { return table_name; }
    const NamesAndTypesList & getColumnsListImpl() const override { return columns; }
    BlockInputStreams read(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size = DEFAULT_BLOCK_SIZE,
        unsigned threads = 1) override;

    void drop() override {}
    static NamesAndTypesList getNamesAndTypes(const DictionaryStructure & dictionary_structure);

private:
    using Ptr = MultiVersion<IDictionaryBase>::Version;

    String table_name;
    String dictionary_name;
    Poco::Logger * logger;

    void checkNamesAndTypesCompatibleWithDictionary(const DictionaryStructure & dictionary_structure) const;

    template <typename ForwardIterator>
    std::string generateNamesAndTypesDescription(ForwardIterator begin, ForwardIterator end) const
    {
        if (begin == end)
        {
            return "";
        }
        std::string description;
        for (; begin != end; ++begin)
        {
            description += ", ";
            description += begin->name;
            description += ' ';
            description += begin->type->getName();
        }
        return description.substr(2, description.size());
    }

protected:
    StorageDictionary(const String & table_name_,
        const NamesAndTypesList & columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        const DictionaryStructure & dictionary_structure_,
        const String & dictionary_name_);
};

}
