#pragma once

#include <map>
#include <memory>
#include <optional>
#include <vector>
#include <base/types.h>

namespace DB
{

class ColumnsDescription;
class ReadBuffer;
class WriteBuffer;
struct IMergeTreeIndex;
using MergeTreeIndexPtr = std::shared_ptr<const IMergeTreeIndex>;

/// Per secondary index, the type names its granules were built against, kept only for required
/// (sub)columns whose built-against type differs from the part's own declared column type. That
/// happens when a skip index is materialized over a column type the part has not materialized itself
/// (for example a JSON type hint applied lazily); otherwise nothing is recorded.
class SecondaryIndexColumnTypes
{
public:
    using ColumnTypes = std::map<String, String>;

    bool empty() const { return index_to_column_types.empty(); }

    /// The type an index's granules were built against for a required column, or nullopt when nothing
    /// was recorded for it (meaning it equals the part's own column type).
    std::optional<String> tryGetBuiltType(const String & index_name, const String & column_name) const;

    void writeJSON(WriteBuffer & out) const;
    static SecondaryIndexColumnTypes readJSON(ReadBuffer & in);

    /// Build the record for a freshly written part: rebuilt indices contribute the types they were
    /// built against where those differ from @new_part_columns; hardlinked indices carry the source
    /// part's entries unchanged (their granules and columns are inherited unchanged). May be empty.
    static SecondaryIndexColumnTypes compute(
        const std::vector<MergeTreeIndexPtr> & rebuilt_indices,
        const std::vector<String> & hardlinked_index_names,
        const ColumnsDescription & new_part_columns,
        const SecondaryIndexColumnTypes & source_part_types);

private:
    std::map<String, ColumnTypes> index_to_column_types;
};

}
