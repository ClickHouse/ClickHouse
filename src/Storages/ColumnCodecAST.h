#pragma once

#include <Storages/ColumnCodecDescription.h>

#include <DataTypes/IDataType_fwd.h>

#include <map>
#include <cstdint>

namespace DB
{

class ASTColumnDeclaration;
struct CodecValidationSettings;

enum class ColumnCodecPatchKind : uint8_t
{
    Set,
    Remove,
};

struct ColumnCodecPatchOperation
{
    ColumnCodecPatchKind kind;
    /// Present only for Set.
    ASTPtr codec;
};

using ColumnCodecPatch = std::map<CodecPath, ColumnCodecPatchOperation>;

/// Read Tuple-element operations and convert Tuple positions to logical element paths.
ColumnCodecPatch tupleElementCodecPatchFromAST(
    const ASTColumnDeclaration & declaration,
    const DataTypePtr & logical_type);

ColumnCodecDescription codecDescriptionFromAST(
    const ASTColumnDeclaration & declaration,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings);

/// Read codec declarations against the explicitly declared type, then validate them against
/// the resulting type. The types differ when NULL handling adds an outer Nullable wrapper.
ColumnCodecDescription codecDescriptionFromAST(
    const ASTColumnDeclaration & declaration,
    const DataTypePtr & declared_type,
    const DataTypePtr & resulting_type,
    const CodecValidationSettings & settings);

void applyCodecDescriptionToAST(
    ASTColumnDeclaration & declaration,
    const DataTypePtr & logical_type,
    const ColumnCodecDescription & codec);

/// Normalize a logical Tuple path against the names used by the owning type.
CodecPath canonicalizeCodecPath(const DataTypePtr & root_type, const CodecPath & input);

/// Return the type reached by a logical Tuple path through the same transparent wrappers.
DataTypePtr getCodecPathType(const DataTypePtr & root_type, const CodecPath & path);

}
