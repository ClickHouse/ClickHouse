#pragma once

namespace DB
{

class ASTTableOverride;
class ASTCreateQuery;

void applyTableOverrideToCreateQuery(const ASTTableOverride & override, ASTCreateQuery * create_query);

}
