#pragma once

#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Parsers/IAST_fwd.h>
#include <Compression/CompressionFactory.h>

#include <map>
#include <vector>

namespace DB
{

class ASTColumnDeclaration;

using CodecPath = std::vector<String>;

struct CodecPathLess
{
    bool operator()(const CodecPath & lhs, const CodecPath & rhs) const;
};

class ColumnCodecDescription
{
public:
    struct Resolved
    {
        ASTPtr ast;
        CodecPath declaration_path;
        bool uses_part_default = true;
    };

    using SubcolumnCodecs = std::map<CodecPath, ASTPtr, CodecPathLess>;

    ColumnCodecDescription() = default;
    ColumnCodecDescription(const ColumnCodecDescription & other);
    ColumnCodecDescription & operator=(const ColumnCodecDescription & other);
    ColumnCodecDescription(ColumnCodecDescription && other) noexcept = default;
    ColumnCodecDescription & operator=(ColumnCodecDescription && other) noexcept = default;
    ColumnCodecDescription(const ASTPtr & root_) { setRoot(root_); } /// NOLINT

    ColumnCodecDescription & operator=(const ASTPtr & ast) { setRoot(ast); return *this; }
    explicit operator bool() const { return !empty(); }

    bool empty() const { return !root && subcolumns.empty(); }
    bool hasRoot() const { return static_cast<bool>(root); }
    bool hasSubcolumns() const { return !subcolumns.empty(); }
    const ASTPtr & getRoot() const { return root; }
    const SubcolumnCodecs & getSubcolumns() const { return subcolumns; }

    void setRoot(const ASTPtr & ast);
    void resetRoot() { root.reset(); }
    void reset() { root.reset(); subcolumns.clear(); }
    void set(CodecPath path, const ASTPtr & ast);
    void erase(const CodecPath & path);

    Resolved resolve(const CodecPath & logical_path, const ASTPtr & part_default) const;
    ColumnCodecDescription clone() const { return ColumnCodecDescription(*this); }
    bool operator==(const ColumnCodecDescription & rhs) const;

private:
    ASTPtr root;
    SubcolumnCodecs subcolumns;
};

struct ResolvedCodecPath
{
    CodecPath path;
    DataTypePtr type;
};

struct TupleCodecRemoval
{
    /// Canonical path in the resulting logical type.
    CodecPath path;
    /// Zero-based tuple positions from the owner to the annotated element.
    std::vector<size_t> positions;
};

struct ColumnCodecPatch
{
    ColumnCodecDescription declarations;
    std::vector<TupleCodecRemoval> removals;
};

ResolvedCodecPath resolveCodecPath(const DataTypePtr & root_type, const CodecPath & input);
CodecPath getCodecPath(const ISerialization::SubstreamPath & path);
CodecPath getCodecPathForStream(
    const NameAndTypePair & written_column,
    const DataTypePtr & owning_type,
    const ISerialization::SubstreamPath & stream_path);

ColumnCodecDescription validateColumnCodecDescription(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings);

ColumnCodecDescription codecDescriptionFromAST(
    const ASTColumnDeclaration & declaration,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings);

/// Extracts ALTER-only tuple-element operations. The root operation remains represented by the
/// ordinary `ASTColumnDeclaration` codec/remove fields.
ColumnCodecPatch codecPatchFromAST(
    const ASTColumnDeclaration & declaration,
    const DataTypePtr & logical_type);

void applyCodecDescriptionToAST(
    ASTColumnDeclaration & declaration,
    const ColumnCodecDescription & codec);

}
