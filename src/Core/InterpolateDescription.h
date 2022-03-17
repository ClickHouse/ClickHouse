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

    explicit InterpolateColumnDescription(const ColumnWithTypeAndName & column_, ExpressionActionsPtr actions_) :
        column(column_), actions(actions_) {}

    bool operator == (const InterpolateColumnDescription & other) const
    {
        return column == other.column;
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
