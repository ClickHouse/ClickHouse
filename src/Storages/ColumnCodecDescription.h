#pragma once

#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Parsers/IAST_fwd.h>
#include <Compression/CompressionFactory.h>

#include <functional>
#include <map>
#include <vector>

/** Codecs are storage metadata for a column. One column can produce many storage streams,
  * and Tuple elements can choose different codecs.
  *
  * This file defines codecs for the whole column and for tuple-element paths.
  * This metadata is not part of IDataType identity or column values.
  */
namespace DB
{

class ASTColumnDeclaration;
class ASTDataType;

/// A logical path of `Tuple` element names relative to the owning top-level column.
using CodecPath = std::vector<String>;

/// Visit Tuple elements through direct Tuple nesting, Array, and SimpleAggregateFunction.
/// Transparent wrappers do not add a codec-path segment.
using TupleCodecElementVisitor = std::function<void(ASTDataType &, const DataTypePtr &, const CodecPath &)>;
void forEachTupleElementInCodecType(
    const ASTPtr & type_ast,
    const DataTypePtr & logical_type,
    CodecPath & path,
    const TupleCodecElementVisitor & visitor);

/** All codec declarations for one column.
  *
  * A tuple path overrides the root or a shorter path. If no declaration matches,
  * the stream uses the part default codec.
  */
class ColumnCodecDescription
{
public:
    /// The codec selected for one logical path after applying inheritance.
    struct Resolved
    {
        ASTPtr codec;
        /// Path of the selected declaration. Empty means the root or the part default.
        CodecPath declaration_path;
        /// True for the part default, including an explicit CODEC(Default).
        bool codec_is_part_default = true;
    };

    /// Explicit declarations for this column. The empty path is the root codec.
    using CodecsByPath = std::map<CodecPath, ASTPtr>;

    ColumnCodecDescription() = default;
    ColumnCodecDescription(const ColumnCodecDescription & other);
    ColumnCodecDescription & operator=(const ColumnCodecDescription & other);
    ColumnCodecDescription(ColumnCodecDescription && other) noexcept = default;
    ColumnCodecDescription & operator=(ColumnCodecDescription && other) noexcept = default;
    ColumnCodecDescription(const ASTPtr & root_) { setRoot(root_); } /// NOLINT

    ColumnCodecDescription & operator=(const ASTPtr & ast) { setRoot(ast); return *this; }
    explicit operator bool() const { return !empty(); }

    bool empty() const { return codecs.empty(); }
    bool hasRoot() const { return codecs.contains(CodecPath{}); }
    bool hasSubcolumns() const { return codecs.size() > static_cast<size_t>(hasRoot()); }
    const ASTPtr & getRoot() const;
    const CodecsByPath & getCodecs() const { return codecs; }

    void setRoot(const ASTPtr & ast);
    void resetRoot() { codecs.erase(CodecPath{}); }
    void reset() { codecs.clear(); }
    void set(CodecPath path, const ASTPtr & ast);
    void erase(const CodecPath & path);

    Resolved resolve(const CodecPath & logical_path, const ASTPtr & part_default) const;
    ColumnCodecDescription clone() const { return ColumnCodecDescription(*this); }
    bool operator==(const ColumnCodecDescription & rhs) const;

private:
    CodecsByPath codecs;
};

CodecPath getCodecPath(const ISerialization::SubstreamPath & path);
CodecPath getCodecPathForStream(
    const NameAndTypePair & written_column,
    const DataTypePtr & owning_type,
    const ISerialization::SubstreamPath & stream_path);

ColumnCodecDescription validateColumnCodecDescription(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings);

/// Validate the complete policy. Apply session settings only to codecs changed by this ALTER.
/// Validate retained codecs as trusted metadata, including their paths and data types.
ColumnCodecDescription validateColumnCodecDescriptionForAlter(
    const ColumnCodecDescription & policy,
    const DataTypePtr & logical_type,
    const ColumnCodecDescription::CodecsByPath & declarations_to_admit,
    const CodecValidationSettings & settings);

ColumnCodecDescription codecDescriptionFromAST(
    const ASTColumnDeclaration & declaration,
    const DataTypePtr & logical_type,
    const CodecValidationSettings & settings);

void applyCodecDescriptionToAST(
    ASTColumnDeclaration & declaration,
    const DataTypePtr & logical_type,
    const ColumnCodecDescription & codec);

}
