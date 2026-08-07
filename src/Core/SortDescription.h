#pragma once


#include <Core/Field.h>
#include <Common/IntervalKind.h>
#include <DataTypes/IDataType.h>
#include <Columns/Collator.h>

#include <cstddef>
#include <string>
#include <memory>
#include <vector>

namespace DB
{

namespace JSONBuilder
{
    class JSONMap;
    class IItem;
    using ItemPtr = std::unique_ptr<IItem>;
}

class Block;

struct FillColumnDescription
{
    /// All missed values in range [FROM, TO) will be filled
    /// Range [FROM, TO) respects sorting direction
    Field fill_from;        /// Fill value >= FILL_FROM
    DataTypePtr fill_from_type;
    Field fill_to;          /// Fill value + STEP < FILL_TO
    DataTypePtr fill_to_type;
    Field fill_step;        /// Default = +1 or -1 according to direction
    std::optional<IntervalKind> step_kind;
    Field fill_staleness;   /// Default = Null - should not be considered
    std::optional<IntervalKind> staleness_kind;

    using StepFunction = std::function<void(Field &, Int32 jumps_count)>;
    StepFunction step_func;
    StepFunction staleness_step_func;
};

/// Description of the sorting rule by one column.
struct SortColumnDescription
{
    std::string column_name; /// The name of the column.
    int direction;           /// 1 - ascending, -1 - descending.
    int nulls_direction;     /// 1 - NULLs and NaNs are greater, -1 - less.
                             /// To achieve NULLS LAST, set it equal to direction, to achieve NULLS FIRST, set it opposite.
    std::shared_ptr<Collator> collator; /// Collator for locale-specific comparison of strings
    bool with_fill;
    FillColumnDescription fill_description;

    SortColumnDescription() = default;

    explicit SortColumnDescription(
        std::string column_name_,
        int direction_ = 1,
        int nulls_direction_ = 1,
        const std::shared_ptr<Collator> & collator_ = nullptr,
        bool with_fill_ = false,
        const FillColumnDescription & fill_description_ = {})
        : column_name(std::move(column_name_))
        , direction(direction_)
        , nulls_direction(nulls_direction_)
        , collator(collator_)
        , with_fill(with_fill_)
        , fill_description(fill_description_)
    {
    }

    static bool compareCollators(const std::shared_ptr<Collator> & a, const std::shared_ptr<Collator> & b)
    {
        if (unlikely(a && b))
            return *a == *b;

        return a == b;
    }

    bool operator==(const SortColumnDescription & other) const
    {
        return column_name == other.column_name && direction == other.direction && nulls_direction == other.nulls_direction
            && compareCollators(collator, other.collator);
    }

    bool operator != (const SortColumnDescription & other) const
    {
        return !(*this == other);
    }

    std::string dump() const { return fmt::format("{}:dir {}nulls {}", column_name, direction, nulls_direction); }

    void explain(JSONBuilder::JSONMap & map) const;
};

struct SortColumnDescriptionWithColumnIndex
{
    SortColumnDescription base;
    size_t column_number;

    SortColumnDescriptionWithColumnIndex(SortColumnDescription description_, size_t column_number_)
        : base(std::move(description_)), column_number(column_number_)
    {
    }

    bool operator==(const SortColumnDescriptionWithColumnIndex & other) const
    {
        return base == other.base && column_number == other.column_number;
    }

    bool operator!=(const SortColumnDescriptionWithColumnIndex & other) const { return !(*this == other); }
};

class CompiledSortDescriptionFunctionHolder;

/// Description of the sorting rule for several columns.
using SortDescriptionWithPositions = std::vector<SortColumnDescriptionWithColumnIndex>;

class SortDescription : public std::vector<SortColumnDescription>
{
public:
    /// Can be safely cast into JITSortDescriptionFunc
    void * compiled_sort_description = nullptr;
    std::shared_ptr<CompiledSortDescriptionFunctionHolder> compiled_sort_description_holder;
    size_t min_count_to_compile_sort_description = 3;
    bool compile_sort_description = false;

    bool hasPrefix(const SortDescription & prefix) const;
};

/// Returns a copy of lhs containing only the prefix of columns matching rhs's columns.
SortDescription commonPrefix(const SortDescription & lhs, const SortDescription & rhs);

/** Compile sort description for header_types.
  * Description is compiled only if compilation attempts to compile identical description is more than min_count_to_compile_sort_description.
  */
void compileSortDescriptionIfNeeded(SortDescription & description, const DataTypes & sort_description_types, bool increase_compile_attempts);

/// Outputs user-readable description into `out`.
void dumpSortDescription(const SortDescription & description, WriteBuffer & out);

std::string dumpSortDescription(const SortDescription & description);

JSONBuilder::ItemPtr explainSortDescription(const SortDescription & description);

class WriteBuffer;
class ReadBuffer;

void serializeSortDescription(const SortDescription & sort_description, WriteBuffer & out);
void deserializeSortDescription(SortDescription & sort_description, ReadBuffer & in);

}
