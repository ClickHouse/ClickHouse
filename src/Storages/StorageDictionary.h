#pragma once

#include <Storages/IStorage.h>
#include <ext/shared_ptr_helper.h>


namespace DB
{
struct DictionaryStructure;

class StorageDictionary final : public ext::shared_ptr_helper<StorageDictionary>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageDictionary>;
public:
    std::string getName() const override { return "Dictionary"; }

    void checkTableCanBeDropped() const override;

    Pipes read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned threads) override;

    static NamesAndTypesList getNamesAndTypes(const DictionaryStructure & dictionary_structure);
    static String generateNamesAndTypesDescription(const NamesAndTypesList & list);

    const String & dictionaryName() const { return dictionary_name; }

private:
    String dictionary_name;

protected:
    StorageDictionary(
        const StorageID & table_id_,
        const String & dictionary_name_,
        const DictionaryStructure & dictionary_structure);
};

}
