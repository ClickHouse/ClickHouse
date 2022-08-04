#pragma once

#include <Core/Block.h>
#include <Columns/ColumnVector.h>
#include <Dictionaries/IDictionary.h>
#include <Interpreters/IKeyValueEntity.h>

namespace DB
{

/// Used in join with dictionary to provide sufficient interface to DirectJoin
class DictionaryJoinAdapter : public IKeyValueEntity
{
public:
    DictionaryJoinAdapter(std::shared_ptr<const IDictionary> dictionary_, const Names & result_column_names);

    Names getPrimaryKey() const override;

    Chunk getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & out_null_map) const override;

    Block getSampleBlock() const override;

private:
    std::shared_ptr<const IDictionary> dictionary;

    Block sample_block;

    Strings attribute_names;
    DataTypes result_types;
};

}
