#pragma once

#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Parsers/IAST_fwd.h>
#include <Compression/CompressionFactory.h>

#include <map>
#include <vector>

/** Compression codecs are storage metadata of an owning column, but one logical column can produce
  * many serialization streams. In particular, elements of a `Tuple` can select codecs independently.
  *
  * This file defines the column-level codec policy: an optional declaration for the whole column,
  * and optional declarations for logical tuple-element paths. None of this state is part of
  * `IDataType` identity or column values.
  */
namespace DB
{

class ASTColumnDeclaration;

/// A logical path of `Tuple` element names relative to the owning top-level column.
using CodecPath = std::vector<String>;

/** The complete codec policy of one owning column.
  *
  * A root declaration applies to the column as a whole. A declaration associated with a tuple path
  * overrides the root or a less-specific ancestor for streams belonging to that element. A stream
  * with no applicable declaration uses the part default codec.
  */
class ColumnCodecDescription
{
public:
    /// The codec source selected for one logical path after applying inheritance.
    struct Resolved
    {
        ASTPtr codec;
        /// Path on which the winning declaration is stored; empty for a root declaration or part default.
        CodecPath declaration_path;
        /// True when the stream follows the part default, including an explicit `CODEC(Default)` declaration.
        bool codec_is_part_default = true;
    };

    /// Explicit declarations belonging to this column. The empty path denotes the column-level codec.
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

/// Validate the complete effective policy, but apply session admission settings only to declarations
/// supplied or changed by the current ALTER. Other declarations are already accepted metadata, so
/// they use trusted admission while still undergoing structural, path, and type-compatibility checks.
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
    const ColumnCodecDescription & codec);

}
