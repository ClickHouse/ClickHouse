#pragma once

#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Replaces specified columns in each chunk with constant values.
/// Used by the http_column_* feature to inject HTTP header values into INSERT columns.
/// For the sync INSERT path, all rows in a single request carry the same header values.
class HTTPHeaderColumnsTransform : public ISimpleTransform
{
public:
    HTTPHeaderColumnsTransform(
        const Block & header_,
        const NameToNameMap & http_header_columns_)
        : ISimpleTransform(header_, header_, false)
    {
        /// Precompute column indices and values for the injected columns.
        for (size_t i = 0; i < header_.columns(); ++i)
        {
            auto it = http_header_columns_.find(header_.getByPosition(i).name);
            if (it != http_header_columns_.end())
                injected_columns.push_back({i, it->second, header_.getByPosition(i).type});
        }
    }

    String getName() const override { return "HTTPHeaderColumnsTransform"; }

protected:
    void transform(Chunk & chunk) override
    {
        if (injected_columns.empty())
            return;

        size_t num_rows = chunk.getNumRows();
        auto columns = chunk.detachColumns();

        for (const auto & [col_idx, value, type] : injected_columns)
        {
            auto new_col = type->createColumn();
            new_col->reserve(num_rows);
            for (size_t row = 0; row < num_rows; ++row)
                new_col->insert(value);
            columns[col_idx] = std::move(new_col);
        }

        chunk.setColumns(std::move(columns), num_rows);
    }

private:
    struct InjectedColumn
    {
        size_t index;
        String value;
        DataTypePtr type;
    };
    std::vector<InjectedColumn> injected_columns;
};

}
