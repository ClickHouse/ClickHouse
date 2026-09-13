#pragma once

#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>

#include <map>
#include <vector>

/** One column can produce many storage streams,
  * and Tuple elements can have different codecs.
  *
  * Defines codecs for the whole column and for tuple-element paths.
  */
namespace DB
{

/// A logical path of `Tuple` element names relative to the owning top-level column.
using CodecPath = std::vector<String>;

/** All codec declarations for one column.
  *
  * A tuple path overrides the root or a shorter path. If no declaration matches,
  * the stream uses the part default codec.
  */
class ColumnCodecDescription
{
public:
    /// The explicit declaration selected by longest-prefix inheritance.
    struct Declaration
    {
        ASTPtr codec;
        /// Empty means the root declaration. A null codec means no declaration matched.
        CodecPath declaration_path;
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

    Declaration find(const CodecPath & logical_path) const;
    ColumnCodecDescription clone() const { return ColumnCodecDescription(*this); }
    bool operator==(const ColumnCodecDescription & rhs) const;

private:
    CodecsByPath codecs;
};

}
