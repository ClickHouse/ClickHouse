#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{

/// True when `ast` is a `CODEC(...)` node whose elements are all names of compression codec families,
/// such as `ZSTD(3)`, `Delta` or `Default`. The names are a static list, because the parser cannot see
/// the codec factory; a test checks the list against the factory.
/// It tells a codec declaration `CODEC name (...)` in a column list from a column named `codec`
/// with a parametrized type, such as `codec Nullable(String)`.
bool isCompressionCodec(const ASTPtr & ast);

}
