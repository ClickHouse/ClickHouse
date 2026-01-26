#pragma once

#include <Parsers/IAST.h>
#include <IO/Operators.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/** Query with output options
  * (supporting [INTO OUTFILE 'file_name'] [FORMAT format_name] [SETTINGS key1 = value1, key2 = value2, ...] suffix).
  */
class ASTQueryWithOutput : public IAST
{
public:
    /// Indices into the children vector for optional output options.
    /// Value of INVALID_INDEX means the option is not set.
    static constexpr UInt8 INVALID_INDEX = 0xFF;

    ASTPtr getOutFile() const { return out_file_index != INVALID_INDEX ? children[out_file_index] : nullptr; }
    ASTPtr getFormatAst() const { return format_ast_index != INVALID_INDEX ? children[format_ast_index] : nullptr; }
    ASTPtr getSettingsAst() const { return settings_ast_index != INVALID_INDEX ? children[settings_ast_index] : nullptr; }
    ASTPtr getCompression() const { return compression_index != INVALID_INDEX ? children[compression_index] : nullptr; }
    ASTPtr getCompressionLevel() const { return compression_level_index != INVALID_INDEX ? children[compression_level_index] : nullptr; }

    void setOutFile(ASTPtr node)
    {
        if (node)
        {
            if (out_file_index != INVALID_INDEX)
                children[out_file_index] = std::move(node);
            else
                out_file_index = addChildAndGetIndex(std::move(node));
        }
        else if (out_file_index != INVALID_INDEX)
            out_file_index = INVALID_INDEX;
    }
    void setFormatAst(ASTPtr node)
    {
        if (node)
        {
            if (format_ast_index != INVALID_INDEX)
                children[format_ast_index] = std::move(node);
            else
                format_ast_index = addChildAndGetIndex(std::move(node));
        }
        else if (format_ast_index != INVALID_INDEX)
            format_ast_index = INVALID_INDEX;
    }
    void setSettingsAst(ASTPtr node)
    {
        if (node)
        {
            if (settings_ast_index != INVALID_INDEX)
                children[settings_ast_index] = std::move(node);
            else
                settings_ast_index = addChildAndGetIndex(std::move(node));
        }
        else if (settings_ast_index != INVALID_INDEX)
            settings_ast_index = INVALID_INDEX;
    }
    void setCompression(ASTPtr node)
    {
        if (node)
        {
            if (compression_index != INVALID_INDEX)
                children[compression_index] = std::move(node);
            else
                compression_index = addChildAndGetIndex(std::move(node));
        }
        else if (compression_index != INVALID_INDEX)
            compression_index = INVALID_INDEX;
    }
    void setCompressionLevel(ASTPtr node)
    {
        if (node)
        {
            if (compression_level_index != INVALID_INDEX)
                children[compression_level_index] = std::move(node);
            else
                compression_level_index = addChildAndGetIndex(std::move(node));
        }
        else if (compression_level_index != INVALID_INDEX)
            compression_level_index = INVALID_INDEX;
    }

    bool isIntoOutfileWithStdout() const { return FLAGS & IS_INTO_OUTFILE_WITH_STDOUT; }
    void setIsIntoOutfileWithStdout(bool value) { FLAGS = value ? (FLAGS | IS_INTO_OUTFILE_WITH_STDOUT) : (FLAGS & ~IS_INTO_OUTFILE_WITH_STDOUT); }

    bool isOutfileAppend() const { return FLAGS & IS_OUTFILE_APPEND; }
    void setIsOutfileAppend(bool value) { FLAGS = value ? (FLAGS | IS_OUTFILE_APPEND) : (FLAGS & ~IS_OUTFILE_APPEND); }

    bool isOutfileTruncate() const { return FLAGS & IS_OUTFILE_TRUNCATE; }
    void setIsOutfileTruncate(bool value) { FLAGS = value ? (FLAGS | IS_OUTFILE_TRUNCATE) : (FLAGS & ~IS_OUTFILE_TRUNCATE); }

    /// Remove 'FORMAT <fmt> and INTO OUTFILE <file>' if exists
    static bool resetOutputASTIfExist(IAST & ast);

    bool hasOutputOptions() const;

protected:
    UInt8 out_file_index = INVALID_INDEX;
    UInt8 format_ast_index = INVALID_INDEX;
    UInt8 settings_ast_index = INVALID_INDEX;
    UInt8 compression_index = INVALID_INDEX;
    UInt8 compression_level_index = INVALID_INDEX;

    /// Bit flags for ASTQueryWithOutput
    static constexpr UInt32 IS_INTO_OUTFILE_WITH_STDOUT = 1u << 0;
    static constexpr UInt32 IS_OUTFILE_APPEND = 1u << 1;
    static constexpr UInt32 IS_OUTFILE_TRUNCATE = 1u << 2;

    UInt8 addChildAndGetIndex(ASTPtr node)
    {
        children.push_back(std::move(node));
        return static_cast<UInt8>(children.size() - 1);
    }

    void resetOutputIndices()
    {
        out_file_index = INVALID_INDEX;
        format_ast_index = INVALID_INDEX;
        settings_ast_index = INVALID_INDEX;
        compression_index = INVALID_INDEX;
        compression_level_index = INVALID_INDEX;
    }

    /// NOTE: call this helper at the end of the clone() method of descendant class.
    void cloneOutputOptions(ASTQueryWithOutput & cloned) const;

    /// Format only the query part of the AST (without output options).
    virtual void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const = 0;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const final;
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
        auto res = make_intrusive<ASTQueryWithOutputImpl<ASTIDAndQueryNames>>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const override
    {
        ostr << ASTIDAndQueryNames::Query;
    }
};

}
