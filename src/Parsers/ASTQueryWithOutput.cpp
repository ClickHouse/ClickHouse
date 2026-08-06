#include <Parsers/ASTQueryWithOutput.h>

#include <Parsers/ASTSetQuery.h>


namespace DB
{

void ASTQueryWithOutput::cloneOutputOptions(ASTQueryWithOutput & cloned) const
{
    if (out_file)
        cloned.set(cloned.out_file, out_file->clone());
    if (format_ast)
        cloned.set(cloned.format_ast, format_ast->clone());
    if (settings_ast)
        cloned.set(cloned.settings_ast, settings_ast->clone());
    if (compression)
        cloned.set(cloned.compression, compression->clone());
    if (compression_level)
        cloned.set(cloned.compression_level, compression_level->clone());
}

void ASTQueryWithOutput::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    /// Let inner nodes (e.g. ASTSelectWithUnionQuery) know that SETTINGS will be
    /// appended after them, so they can parenthesize the last SELECT to prevent the
    /// re-parser from consuming SETTINGS into that SELECT.
    /// When out_file or format_ast is present, they are formatted before SETTINGS and
    /// act as a separator, so the parser stops before them — no parentheses needed.
    frame.parent_has_trailing_settings = frame.parent_has_trailing_settings || (settings_ast && !out_file && !format_ast);

    /// Let inner nodes (e.g. ASTCreateQuery) know that trailing output options
    /// will follow, so they can parenthesize AS-select if needed.
    if (hasOutputOptions())
        frame.has_trailing_output_options = true;

    formatQueryImpl(ostr, s, state, frame);

    std::string indent_str = s.one_line ? "" : std::string(4u * frame.indent, ' ');

    if (out_file)
    {
        ostr << s.nl_or_ws << indent_str << "INTO OUTFILE ";
        out_file->format(ostr, s, state, frame);

        if (isOutfileAppend())
            ostr << " APPEND";
        if (isOutfileTruncate())
            ostr << " TRUNCATE";
        if (isIntoOutfileWithStdout())
            ostr << " AND STDOUT";
        if (compression)
        {
            ostr << " COMPRESSION ";
            compression->format(ostr, s, state, frame);
        }
        if (compression_level)
        {
            ostr << indent_str << " LEVEL ";
            compression_level->format(ostr, s, state, frame);
        }
    }

    if (format_ast)
    {
        ostr << s.nl_or_ws << indent_str << "FORMAT ";
        format_ast->format(ostr, s, state, frame);
    }

    if (settings_ast)
    {
        ostr << s.nl_or_ws << indent_str << "SETTINGS ";
        settings_ast->format(ostr, s, state, frame);
    }
}

bool ASTQueryWithOutput::resetOutputASTIfExist(IAST & ast)
{
    /// FIXME: try to prettify this cast using `as<>()`
    if (auto * ast_with_output = dynamic_cast<ASTQueryWithOutput *>(&ast))
    {
        ast_with_output->reset(ast_with_output->out_file);
        ast_with_output->reset(ast_with_output->format_ast);
        ast_with_output->reset(ast_with_output->settings_ast);
        ast_with_output->reset(ast_with_output->compression);
        ast_with_output->reset(ast_with_output->compression_level);

        return true;
    }

    return false;
}

bool ASTQueryWithOutput::hasOutputOptions() const
{
    return out_file || format_ast || settings_ast || compression || compression_level;
}

}
