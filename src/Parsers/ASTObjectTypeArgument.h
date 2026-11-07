#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/// A pair of Object path and its data type, for example `a.b.c String`, optionally marked `SHARED`.
class ASTObjectTypedPathArgument : public IAST
{
public:
    /// path
    String path;
    /// type
    ASTPtr type;
    /// `SHARED path Type`: the path is stored in the typed shared data of a part instead of its own files.
    bool shared = false;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return String(shared ? "ObjectSharedTypedPath" : "ObjectTypedPath") + delim + path; }
    ASTPtr clone() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

/** An argument of Object data type declaration (for example for JSON). Can contain one of:
 *  - pair (path, data type)
 *  - path that should be skipped
 *  - path regexp for paths that should be skipped
 *  - path regexp for paths that should be stored in shared data
 *  - setting in a form of `setting=N`
 */
class ASTObjectTypeArgument : public IAST
{
public:
    ASTPtr path_with_type;
    ASTPtr skip_path;
    ASTPtr skip_path_regexp;
    ASTPtr shared_path_regexp;
    ASTPtr parameter;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "ASTObjectTypeArgument"; }
    ASTPtr clone() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


}

