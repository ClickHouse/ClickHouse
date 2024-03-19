#pragma once

#include <cstdint>
#include <Parsers/IAST.h>
#include <IO/Operators.h>
#include "Core/Settings.h"
#include "Parsers/IAST_fwd.h"


namespace DB
{

/** Query with output options
  * (supporting [INTO OUTFILE 'file_name'] [FORMAT format_name] [SETTINGS key1 = value1, key2 = value2, ...] suffix).
  */
class ASTQueryWithOutput : public IAST
{
public:
    IAST * out_file = nullptr;
    bool is_into_outfile_with_stdout = false;
    bool is_outfile_append = false;
    bool is_outfile_truncate = false;
    IAST * format = nullptr;
    IAST * settings_ast = nullptr;
    IAST * compression = nullptr;
    IAST * compression_level = nullptr;

    /// Just for convenience.
    void setOutFile(const ASTPtr & out_file_) { set(out_file, out_file_); }
    void setFormat(const ASTPtr & format_) { set(format, format_); }
    void setSettingsAST(const ASTPtr & settings_ast_) { set(settings_ast, settings_ast_); }
    void setCompression(const ASTPtr & compression_) { set(compression, compression_); }
    void setCompressionLevel(const ASTPtr & compression_level_) { set(compression_level, compression_level_); }

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const final;

    /// Remove 'FORMAT <fmt> and INTO OUTFILE <file>' if exists
    static bool resetOutputASTIfExist(IAST & ast);

protected:
    /// NOTE: call this helper at the end of the clone() method of descendant class.
    void cloneOutputOptions(ASTQueryWithOutput & cloned) const;

    /// Format only the query part of the AST (without output options).
    virtual void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const = 0;

    void forEachPointerToChild(std::function<void(void**)> f) override;
};


/** Helper template for simple queries like SHOW PROCESSLIST.
  */
template <typename ASTIDAndQueryNames>
class ASTQueryWithOutputImpl : public ASTQueryWithOutput
{
public:
    String getID(char) const override { return ASTIDAndQueryNames::ID; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTQueryWithOutputImpl<ASTIDAndQueryNames>>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "")
            << ASTIDAndQueryNames::Query << (settings.hilite ? hilite_none : "");
    }
};

}
