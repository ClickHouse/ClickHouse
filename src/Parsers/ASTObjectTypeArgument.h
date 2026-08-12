#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/// A pair of  Object path and its data type. For example: a.b.c String.
class ASTObjectTypedPathArgument : public IAST
{
public:
    /// path
    String path;
    /// type
    ASTPtr type;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "ObjectTypedPath" + (delim + path); }
    ASTPtr clone() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

/** An argument of Object data type declaration (for example for JSON). Can contain one of:
 *  - pair (path, data type)
 *  - path that should be skipped
 *  - path regexp for paths that should be skipped
 *  - path regexp for paths that must always be stored in shared data
 *  - setting in a form of `setting=N`
 */
class ASTObjectTypeArgument : public IAST
{
public:
    ASTPtr path_with_type;
    ASTPtr skip_path;
    ASTPtr skip_path_regexp;
    ASTPtr shared_path_regexp;
    /// `SHARED REGEXP FULL '...'` is used when a persisted part contains full-match rules.
    /// User-facing declarations normally select this mode with
    /// `shared_regexp_use_partial_match=0`.
    bool shared_path_regexp_full_match = false;
    ASTPtr parameter;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "ASTObjectTypeArgument"; }
    ASTPtr clone() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


}
