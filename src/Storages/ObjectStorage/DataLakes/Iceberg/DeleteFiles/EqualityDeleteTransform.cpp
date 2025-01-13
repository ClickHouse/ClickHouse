#include <Storages/ObjectStorage/DataLakes/Iceberg/DeleteFiles/EqualityDeleteTransform.h>

#include <Core/Field.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

bool EqualityDeleteKey::operator==(const EqualityDeleteKey & key) const
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

size_t EqualityDeleteKeyHasher::operator()(const EqualityDeleteKey & key) const
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

EqualityDeleteTransform::EqualityDeleteTransform(const Block & header, std::shared_ptr<IInputFormat> delete_file_source_)
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

void EqualityDeleteTransform::transform(Chunk & chunk)
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

}
