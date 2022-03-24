#pragma once

#include <unordered_map>
#include <memory>
#include <cstddef>
#include <string>
#include <Core/Field.h>
#include <Core/SettingsEnums.h>
#include <Common/IntervalKind.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Functions/FunctionsMiscellaneous.h>


namespace DB
{

/// Interpolate description
struct InterpolateDescription
{
    explicit InterpolateDescription(ExpressionActionsPtr actions);

    ExpressionActionsPtr actions;
    std::set<std::string> columns_full_set; /// columns to add to row
    std::unordered_map<std::string, NameAndTypePair> required_columns_map; /// input columns
    std::unordered_map<std::string, size_t> result_columns_map; /// result block column name -> block column index

    /// filled externally in transform
    std::unordered_map<size_t, NameAndTypePair> input_map; /// row index -> column name type
    std::unordered_map<size_t, size_t> output_map; /// result block column index -> row index
    std::unordered_map<size_t, DataTypePtr> reset_map; /// row index -> column type, columns not filled by fill or interpolate
};

using InterpolateDescriptionPtr = std::shared_ptr<InterpolateDescription>;

}
