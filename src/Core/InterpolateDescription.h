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
#include <Interpreters/Aliases.h>


namespace DB
{

/// Interpolate description
struct InterpolateDescription
{
    explicit InterpolateDescription(ActionsDAGPtr actions, const Aliases & aliases);

    ActionsDAGPtr actions;

    std::unordered_map<std::string, NameAndTypePair> required_columns_map; /// input column name -> {alias, type}
    std::unordered_set<std::string> result_columns_set; /// result block columns
    std::vector<std::string> result_columns_order; /// result block columns order
};

using InterpolateDescriptionPtr = std::shared_ptr<InterpolateDescription>;

}
