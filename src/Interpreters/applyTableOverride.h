#pragma once

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTableOverrides.h>

namespace DB
{

class ASTTableOverride;
class ASTCreateQuery;
class ASTIndentifier;

void applyTableOverrideToCreateQuery(const ASTTableOverride & override, ASTCreateQuery * create_query);

}
