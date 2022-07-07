#pragma once

#include <Core/Block.h>
#include <Columns/ColumnVector.h>
#include <Dictionaries/IDictionary.h>
#include <Storages/IKVStorage.h>

namespace DB
{

/// Used in join with dictionary to provide sufficient interface to DirectJoin
class DictionaryJoinAdapter : public IKeyValueStorage
{
public:
    DictionaryJoinAdapter(
        std::shared_ptr<const IDictionary> dictionary_, const Names & result_column_names);

    Names getPrimaryKey() const override;

    Chunk getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & out_null_map) const override;

    std::string getName() const override
    {
        return dictionary->getFullName();
    }

private:
    std::shared_ptr<const IDictionary> dictionary;

    Strings attribute_names;
    DataTypes result_types;
};

}
