#pragma once

#include <Core/Types.h>
#include <DataTypes/IDataType_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <map>
#include <set>
#include <vector>

namespace DB
{

class ASTIdentifier;
class ColumnsDescription;

/// User-visible Tuple element names relative to one physical owning column.
/// A component can contain a literal dot and must never be split after parsing.
using TupleCodecPath = std::vector<String>;

/// Explicit element declarations for one physical column. The root declaration
/// remains in ColumnDescription::codec and therefore the empty path is invalid here.
using TupleElementCodecs = std::map<TupleCodecPath, ASTPtr>;

/// Sparse schema-level storage. Only owners with element declarations are present.
using TupleElementCodecsByColumn = std::map<String, TupleElementCodecs>;

/// Used by ALTER validation. Unlike TupleElementCodecs, the empty path is valid
/// and identifies the owning column's root declaration.
using CodecDeclarationPaths = std::set<TupleCodecPath>;

struct ResolvedCodecTarget
{
    String owner_name;
    TupleCodecPath path;

    bool isRoot() const { return path.empty(); }
};

/// Resolve physical-column precedence and then canonicalize a remaining Tuple path.
ResolvedCodecTarget resolveCodecTarget(
    const ColumnsDescription & columns,
    const ASTIdentifier & target);

/// Canonicalize names, numeric names of unnamed Tuple elements, and transparent wrappers.
TupleCodecPath canonicalizeTupleCodecPath(
    const DataTypePtr & owner_type,
    const TupleCodecPath & input);

}

