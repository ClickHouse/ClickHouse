#pragma once

#include <Storages/ColumnCodecPath.h>

#include <optional>

namespace DB
{

/** A lightweight view of all codec declarations belonging to one physical column.
  *
  * It keeps an intrusive-pointer copy of the root AST and borrows the element
  * map from the sparse ColumnsDescription side table. It must not outlive that schema.
  */
class ColumnCodecPolicyView
{
public:
    struct Declaration
    {
        ASTPtr codec;
        /// Empty for the root declaration.
        TupleCodecPath declaration_path;
    };

    ColumnCodecPolicyView(
        ASTPtr root_codec_,
        const TupleElementCodecs * element_codecs_ = nullptr);

    bool hasElements() const;
    const ASTPtr & getRoot() const { return root_codec; }
    const TupleElementCodecs * getElements() const { return element_codecs; }

    /// Deepest element prefix wins, followed by the root declaration.
    std::optional<Declaration> find(const TupleCodecPath & stream_path) const;

private:
    /// Keep an intrusive-pointer copy so construction from a cloned AST is safe.
    ASTPtr root_codec;
    const TupleElementCodecs * element_codecs;
};

}
