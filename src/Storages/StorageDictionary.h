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

    /// Specifies where the table is located relative to the dictionary.
    enum class Location
    {
        /// Table was created automatically as an element of a database with the Dictionary engine.
        DictionaryDatabase,

        /// Table was created automatically along with a dictionary
        /// and has the same database and name as the dictionary.
        /// It provides table-like access to the dictionary.
        /// User cannot drop that table.
        SameDatabaseAndNameAsDictionary,

        /// Table was created explicitly by a statement like
        /// CREATE TABLE ... ENGINE=Dictionary
        /// User chose the table's database and name and can drop that table.
        Custom,
    };

private:
    const String dictionary_name;
    const Location location;

protected:
    StorageDictionary(
        const StorageID & table_id_,
        const String & dictionary_name_,
        const ColumnsDescription & columns_,
        Location location_);

    StorageDictionary(
        const StorageID & table_id_,
        const String & dictionary_name_,
        const DictionaryStructure & dictionary_structure,
        Location location_);
};

}
