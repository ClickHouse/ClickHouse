#pragma once

namespace DB
{

class ASTColumnDeclaration;

/// DEFAULT expressions written inside a Tuple data type (e.g. `Tuple(a UInt8, s String DEFAULT 'Hello')`)
/// exist only at the syntax level. This pulls them up to the column level, normalizing the stored type to
/// one without any DEFAULTs and setting an equivalent column-level `DEFAULT tuple(...)` expression. Elements
/// without an explicit DEFAULT are filled with `defaultValueOfTypeName('<type>')`.
///
/// No-op for column declarations whose type contains no such DEFAULTs. Shared by the `CREATE TABLE` path and
/// the `ALTER TABLE ... ADD COLUMN` / `MODIFY COLUMN` path, so all of them normalize identically.
/// See https://github.com/ClickHouse/ClickHouse/issues/2797.
void pullUpTupleElementDefaults(ASTColumnDeclaration & col_decl);

}
