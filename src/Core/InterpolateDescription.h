#pragma once

#include <vector>
#include <memory>
#include <cstddef>
#include <string>
#include <Core/Field.h>
#include <Core/SettingsEnums.h>
#include <Common/IntervalKind.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Functions/FunctionsMiscellaneous.h>

class Collator;

namespace DB
{

namespace JSONBuilder
{
    class JSONMap;
    class IItem;
    using ItemPtr = std::unique_ptr<IItem>;
}

class Block;


/// Interpolate description
struct InterpolateColumnDescription
{
    using Signature = ExecutableFunctionExpression::Signature;

    ColumnWithTypeAndName column;
    ExpressionActionsPtr actions;
  //  ExecutableFunctionExpression interpolate_func;
   // std::string column_name; /// The name of the column.
 //   size_t column_number;    /// Column number (used if no name is given).



    explicit InterpolateColumnDescription(const ColumnWithTypeAndName & column_, ExpressionActionsPtr actions_) :
        column(column_), actions(actions_) {}

  //  explicit InterpolateColumnDescription(size_t column_number_, const ASTInterpolateElement & /*ast*/) : column_number(column_number_) {}

 //   explicit InterpolateColumnDescription(const std::string & column_name_, const ASTInterpolateElement & /*ast*/) : column_name(column_name_), column_number(0) {}

    bool operator == (const InterpolateColumnDescription & other) const
    {
        return column == other.column;// && column_number == other.column_number;
    }

    bool operator != (const InterpolateColumnDescription & other) const
    {
        return !(*this == other);
    }

    void interpolate(Field & field) const;

    std::string dump() const
    {
        return fmt::format("{}", column.name);
    }

    void explain(JSONBuilder::JSONMap & map, const Block & header) const;
};

/// Description of interpolation for several columns.
using InterpolateDescription = std::vector<InterpolateColumnDescription>;

/// Outputs user-readable description into `out`.
void dumpInterpolateDescription(const InterpolateDescription & description, const Block & header, WriteBuffer & out);

std::string dumpInterpolateDescription(const InterpolateDescription & description);

JSONBuilder::ItemPtr explainInterpolateDescription(const InterpolateDescription & description, const Block & header);

}
