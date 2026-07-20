#include <Parsers/ASTQueryWithOutput.h>

#include <Parsers/ASTSetQuery.h>


namespace DB
{

void ASTQueryWithOutput::cloneOutputOptions(ASTQueryWithOutput & cloned) const
{
    if (out_file)
    {
        cloned.out_file = out_file->clone();
        cloned.children.push_back(cloned.out_file);
    }
    if (format_ast)
    {
        cloned.format_ast = format_ast->clone();
        cloned.children.push_back(cloned.format_ast);
    }
    if (settings_ast)
    {
        cloned.settings_ast = settings_ast->clone();
        cloned.children.push_back(cloned.settings_ast);
    }
    if (compression)
    {
        cloned.compression = compression->clone();
        cloned.children.push_back(cloned.compression);
    }
    if (compression_level)
    {
        cloned.compression_level = compression_level->clone();
        cloned.children.push_back(cloned.compression_level);
    }
}

void ASTQueryWithOutput::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    formatQueryImpl(ostr, s, state, frame);

    std::string indent_str = s.one_line ? "" : std::string(4u * frame.indent, ' ');

    if (out_file)
    {
        ostr << s.nl_or_ws << indent_str << "INTO OUTFILE ";
        out_file->format(ostr, s, state, frame);

        if (is_outfile_append)
            ostr << " APPEND";
        if (is_outfile_truncate)
            ostr << " TRUNCATE";
        if (is_into_outfile_with_stdout)
            ostr << " AND STDOUT";
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
        auto remove_if_exists = [&](ASTPtr & p)
        {
            if (p)
            {
                if (auto * it = std::find(ast_with_output->children.begin(), ast_with_output->children.end(), p);
                    it != ast_with_output->children.end())
                    ast_with_output->children.erase(it);
                p.reset();
            }
        };

        remove_if_exists(ast_with_output->out_file);
        remove_if_exists(ast_with_output->format_ast);
        remove_if_exists(ast_with_output->settings_ast);
        remove_if_exists(ast_with_output->compression);
        remove_if_exists(ast_with_output->compression_level);

        return true;
    }

    return false;
}

bool ASTQueryWithOutput::hasOutputOptions() const
{
    return out_file || format_ast || settings_ast || compression || compression_level;
}

}
