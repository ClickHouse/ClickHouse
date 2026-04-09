#include <Processors/Transforms/TopologicalSortTransform.h>

#include <numeric>
#include <queue>
#include <unordered_map>

#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorHash.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

struct FieldHash
{
    size_t operator()(const Field & f) const
    {
        SipHash hash;
        applyVisitor(FieldVisitorHash{hash}, f);
        return hash.get64();
    }
};

} // anonymous namespace

TopologicalSortTransform::TopologicalSortTransform(
    SharedHeader header,
    const String & key_column_name_,
    const String & deps_column_name_)
    : IAccumulatingTransform(header, header)
    , key_column_name(key_column_name_)
    , deps_column_name(deps_column_name_)
    , key_column_pos(header->getPositionByName(key_column_name))
    , deps_column_pos(header->getPositionByName(deps_column_name))
{
}

void TopologicalSortTransform::consume(Chunk chunk)
{
    if (chunk.getNumRows() > 0)
        accumulated.push_back(std::move(chunk));
}

Chunk TopologicalSortTransform::generate()
{
    if (generated)
        return {};
    generated = true;

    if (accumulated.empty())
        return {};

    // Merge all chunks
    size_t total_rows = 0;
    for (const auto & chunk : accumulated)
        total_rows += chunk.getNumRows();

    const size_t num_cols = accumulated.front().getNumColumns();
    MutableColumns merged(num_cols);
    for (size_t col = 0; col < num_cols; ++col)
    {
        merged[col] = accumulated.front().getColumns()[col]->cloneEmpty();
        merged[col]->reserve(total_rows);
    }
    for (auto & chunk : accumulated)
    {
        auto cols = chunk.detachColumns();
        for (size_t col = 0; col < num_cols; ++col)
            merged[col]->insertRangeFrom(*cols[col], 0, cols[col]->size());
    }
    accumulated.clear();

    // Unwrap ColumnConst (can happen for literals in SELECT)
    for (size_t col = 0; col < num_cols; ++col)
        merged[col] = IColumn::mutate(merged[col]->convertToFullColumnIfConst());

    const IColumn & key_col = *merged[key_column_pos];
    const IColumn * deps_col_raw = merged[deps_column_pos].get();

    // deps column must be ColumnArray
    const auto * deps_arr = typeid_cast<const ColumnArray *>(deps_col_raw);
    if (!deps_arr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DEPENDS ON column '{}' must be of Array type", deps_column_name);

    const auto & offsets = deps_arr->getOffsets();
    const IColumn & deps_data = deps_arr->getData();

    // Build graph: key_value → in_degree, key_value → list of row indices
    using KeyMap = std::unordered_map<Field, std::vector<size_t>, FieldHash>;
    using DegreeMap = std::unordered_map<Field, size_t, FieldHash>;
    using AdjMap = std::unordered_map<Field, std::vector<Field>, FieldHash>;

    KeyMap key_to_rows;
    DegreeMap in_degree;
    AdjMap adjacency; // dep → dependents

    key_to_rows.reserve(total_rows);
    in_degree.reserve(total_rows);

    Field key_field;
    for (size_t row = 0; row < total_rows; ++row)
    {
        key_col.get(row, key_field);
        key_to_rows[key_field].push_back(row);
        in_degree.try_emplace(key_field, 0);
    }

    Field dep_field;
    for (size_t row = 0; row < total_rows; ++row)
    {
        key_col.get(row, key_field);

        const size_t d_start = row == 0 ? 0 : offsets[row - 1];
        const size_t d_end = offsets[row];

        for (size_t d = d_start; d < d_end; ++d)
        {
            deps_data.get(d, dep_field);
            if (dep_field.isNull())
                continue;

            // Only create an edge if the dependency is present in the result set
            if (key_to_rows.contains(dep_field))
            {
                adjacency[dep_field].push_back(key_field);
                in_degree[key_field]++;
            }
        }
    }

    // Kahn's BFS
    std::queue<Field> queue;
    for (const auto & [key, degree] : in_degree)
        if (degree == 0)
            queue.push(key);

    IColumn::Permutation permutation;
    permutation.reserve(total_rows);

    while (!queue.empty())
    {
        Field current = std::move(queue.front());
        queue.pop();

        if (auto it = key_to_rows.find(current); it != key_to_rows.end())
            for (size_t r : it->second)
                permutation.push_back(r);

        if (auto it = adjacency.find(current); it != adjacency.end())
        {
            for (const auto & dep : it->second)
            {
                if (--in_degree[dep] == 0)
                    queue.push(dep);
            }
        }
    }

    if (permutation.size() != total_rows)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cycle detected in ORDER BY ... DEPENDS ON: "
            "{} of {} rows could not be topologically sorted",
            total_rows - permutation.size(), total_rows);

    Columns result(num_cols);
    for (size_t col = 0; col < num_cols; ++col)
        result[col] = merged[col]->permute(permutation, 0);

    return Chunk(std::move(result), total_rows);
}

}
