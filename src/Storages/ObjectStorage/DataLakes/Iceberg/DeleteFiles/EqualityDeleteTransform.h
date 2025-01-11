#pragma once

#include <unordered_set>
#include <Processors/ISimpleTransform.h>
#include "Common/Exception.h"
#include "Common/SipHash.h"
#include <Common/PODArray.h>
#include "Columns/IColumn.h"
#include "Core/Field.h"
#include "Processors/Formats/IInputFormat.h"
#include "base/types.h"


namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace DB
{

class EqualityDeleteKey
{
public:
    EqualityDeleteKey(const Columns & data_, size_t row_) : data(data_), row(row_) { }

    bool operator==(const EqualityDeleteKey & key) const
    {
        if (key.data.size() != data.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Equality delete keys are not compatible");

        for (size_t i = 0; i < data.size(); ++i)
        {
            Field field_left;
            data[i]->get(row, field_left);

            Field field_right;
            key.data[i]->get(key.row, field_right);

            if (field_left != field_right)
                return false;
        }
        return true;
    }

private:
    Columns data;
    size_t row;

    friend struct EqualityDeleteKeyHasher;
};


struct EqualityDeleteKeyHasher
{
    size_t operator()(const EqualityDeleteKey & key) const
    {
        SipHash hash;
        for (size_t i = 0; i < key.data.size(); ++i)
        {
            Field field;
            key.data[i]->get(key.row, field);
            hash.update(field.dump());
        }
        return hash.get64();
    }
};

class EqualityDeleteTransform : public ISimpleTransform
{
public:
    EqualityDeleteTransform(const Block & header, std::shared_ptr<IInputFormat> delete_file_source_)
        : ISimpleTransform(header, header, false), delete_header(delete_file_source_->getOutputs().back().getHeader())
    {
        for (size_t i = 0; i < header.getNames().size(); ++i)
        {
            column_to_index[header.getNames()[i]] = i;
        }
        while (true)
        {
            Chunk chunk = delete_file_source_->generate();
            if (!chunk)
                break;

            for (size_t i = 0; i < chunk.getNumRows(); ++i)
            {
                delete_values.insert(EqualityDeleteKey(chunk.getColumns(), i));
            }
        }
    }

    String getName() const override { return "EqualityDeleteTransform"; }
    void setDescription(const String & str) { description = str; }

protected:
    void transform(Chunk & chunk) override
    {
        UInt64 num_rows = chunk.getNumRows();
        UInt64 num_rows_after_filtration = num_rows;
        IColumn::Filter filter(num_rows, true);
        Columns key_columns;
        auto columns = chunk.detachColumns();

        for (size_t i = 0; i < delete_header.columns(); ++i)
        {
            auto column_name = delete_header.getNames()[i];
            key_columns.push_back(columns[column_to_index[column_name]]);
        }

        for (UInt64 i = 0; i < num_rows; ++i)
        {
            if (delete_values.contains(EqualityDeleteKey(key_columns, i)))
            {
                filter[i] = false;
                --num_rows_after_filtration;
            }
        }

        for (auto & column : columns)
            column = column->filter(filter, -1);

        chunk.setColumns(std::move(columns), num_rows_after_filtration);
    }

private:
    String description;
    std::unordered_set<EqualityDeleteKey, EqualityDeleteKeyHasher> delete_values;
    Block delete_header;
    std::unordered_map<std::string, size_t> column_to_index;
};

}
