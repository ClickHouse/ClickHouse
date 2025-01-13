#pragma once

#include <unordered_set>

#include <Columns/IColumn.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/ISimpleTransform.h>
#include <base/types.h>
#include <Common/PODArray.h>

namespace DB
{

class EqualityDeleteKey
{
public:
    EqualityDeleteKey(const Columns & data_, size_t row_) : data(data_), row(row_) { }

    bool operator==(const EqualityDeleteKey & key) const;

private:
    Columns data;
    size_t row;

    friend struct EqualityDeleteKeyHasher;
};


struct EqualityDeleteKeyHasher
{
    size_t operator()(const EqualityDeleteKey & key) const;
};

class EqualityDeleteTransform : public ISimpleTransform
{
public:
    EqualityDeleteTransform(const Block & header, std::shared_ptr<IInputFormat> delete_file_source_);

    String getName() const override { return "EqualityDeleteTransform"; }
    void setDescription(const String & str) { description = str; }

protected:
    void transform(Chunk & chunk) override;

private:
    String description;
    std::unordered_set<EqualityDeleteKey, EqualityDeleteKeyHasher> delete_values;
    Block delete_header;
    std::unordered_map<std::string, size_t> column_to_index;
};

}
