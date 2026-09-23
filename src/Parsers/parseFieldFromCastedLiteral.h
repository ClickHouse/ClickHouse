#pragma once

#include <Core/Field.h>
#include <Parsers/IAST_fwd.h>

#include <string_view>


namespace DB
{

/// Parses a Field value from an AST node. Inverse of `FieldVisitorToCastedLiteral`. Accepts:
///   - ASTLiteral — returns `literal->value` directly.
///   - ASTFunction `CAST(<literal-value>, <string-literal-type-name>)` — parses the casted
///     literal back into a Field of the right type.
///   - ASTFunction `array(...)` / `tuple(...)`: parses every argument recursively. This is the
///     shape a composite value takes when an element of it carries a cast; an all-literal one
///     parses as a single ASTLiteral instead. A Map prints in the array form and reads back as
///     an Array, as it does in `FieldVisitorToString` too.
///
/// Recognised target type names inside the CAST shape:
///   - Bool, Float64, String
///   - Int64/UInt64, Int128/UInt128, Int256/UInt256
///   - Decimal{32,64,128,256}(<scale>)
///   - UUID, IPv4, IPv6
///
/// Throws BAD_ARGUMENTS if the AST shape doesn't match any of those forms, or if the casted
/// type name is not in the recognised set.
Field parseFieldFromCastedLiteral(const ASTPtr & ast);

}
