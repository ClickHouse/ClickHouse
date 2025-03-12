#pragma once

#include <vector>
#include <Core/NamesAndTypes.h>
#include <Core/Range.h>
#include <Parsers/IAST_fwd.h>

namespace Iceberg
{

DB::ASTPtr getASTFromTransform(const String & transform_name, const String & column_name);

}
