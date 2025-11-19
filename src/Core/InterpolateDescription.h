#pragma once

#include <unordered_map>
#include <memory>
#include <string>
#include <Core/NamesAndTypes.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/ActionsDAG.h>


namespace DB
{

using Aliases = std::unordered_map<String, ASTPtr>;

/// Interpolate description
struct InterpolateDescription
{
    explicit InterpolateDescription(ActionsDAG actions, const Aliases & aliases);

    ActionsDAG actions;

    std::unordered_map<std::string, NameAndTypePair> required_columns_map; /// input column name -> {alias, type}
    std::unordered_set<std::string> result_columns_set; /// result block columns
    std::vector<std::string> result_columns_order; /// result block columns order
};

using InterpolateDescriptionPtr = std::shared_ptr<InterpolateDescription>;

}
