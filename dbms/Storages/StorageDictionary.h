#pragma once

#include <Storages/IStorage.h>
#include <Core/Defines.h>
#include <Common/MultiVersion.h>
#include <ext/shared_ptr_helper.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace Poco
{
class Logger;
}

namespace DB
{
struct DictionaryStructure;
struct IDictionaryBase;
class ExternalDictionaries;

class StorageDictionary final : public ext::shared_ptr_helper<StorageDictionary>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageDictionary>;
public:
    std::string getName() const override { return "Dictionary"; }

    Pipes read(const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned threads) override;

    static NamesAndTypesList getNamesAndTypes(const DictionaryStructure & dictionary_structure);

    template <typename ForwardIterator>
    static std::string generateNamesAndTypesDescription(ForwardIterator begin, ForwardIterator end)
    {
        std::string description;
        {
            WriteBufferFromString buffer(description);
            bool first = true;
            for (; begin != end; ++begin)
            {
                if (!first)
                    buffer << ", ";
                first = false;

                buffer << begin->name << ' ' << begin->type->getName();
            }
        }

        return description;
    }

private:
    using Ptr = MultiVersion<IDictionaryBase>::Version;

    String dictionary_name;
    Poco::Logger * logger;

    void checkNamesAndTypesCompatibleWithDictionary(const DictionaryStructure & dictionary_structure) const;

protected:
    StorageDictionary(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const Context & context,
        bool attach,
        const String & dictionary_name_);
};

}
