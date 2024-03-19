#include <Parsers/ASTQueryWithOutput.h>

#include <Common/assert_cast.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{

void ASTQueryWithOutput::cloneOutputOptions(ASTQueryWithOutput & cloned) const
{
    if (out_file)
        cloned.setOutFile(out_file->clone());
    if (format)
        cloned.setFormat(format->clone());
    if (settings_ast)
        cloned.setSettingsAST(settings_ast->clone());
    if (compression)
        cloned.setCompression(compression->clone());
    if (compression_level)
        cloned.setCompressionLevel(compression_level->clone());
}

void ASTQueryWithOutput::forEachPointerToChild(std::function<void(void**)> f)
{
    f(reinterpret_cast<void **>(&out_file));
    f(reinterpret_cast<void **>(&format));
    f(reinterpret_cast<void **>(&settings_ast));
    f(reinterpret_cast<void **>(&compression));
    f(reinterpret_cast<void **>(&compression_level));
}

void ASTQueryWithOutput::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    formatQueryImpl(s, state, frame);

    std::string indent_str = s.one_line ? "" : std::string(4u * frame.indent, ' ');

    if (out_file)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "INTO OUTFILE " << (s.hilite ? hilite_none : "");
        out_file->formatImpl(s, state, frame);

        s.ostr << (s.hilite ? hilite_keyword : "");
        if (is_outfile_append)
            s.ostr << " APPEND";
        if (is_outfile_truncate)
            s.ostr << " TRUNCATE";
        if (is_into_outfile_with_stdout)
            s.ostr << " AND STDOUT";
        s.ostr << (s.hilite ? hilite_none : "");
    }

    if (format)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "FORMAT " << (s.hilite ? hilite_none : "");
        format->formatImpl(s, state, frame);
    }

    if (settings_ast && assert_cast<ASTSetQuery *>(settings_ast)->print_in_format)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "SETTINGS " << (s.hilite ? hilite_none : "");
        settings_ast->formatImpl(s, state, frame);
    }
}

bool ASTQueryWithOutput::resetOutputASTIfExist(IAST & ast)
{
    if (auto * ast_with_output = typeid_cast<ASTQueryWithOutput *>(&ast))
    {
        ast_with_output->reset(ast_with_output->out_file);
        ast_with_output->reset(ast_with_output->format);
        ast_with_output->reset(ast_with_output->settings_ast);
        ast_with_output->reset(ast_with_output->compression);
        ast_with_output->reset(ast_with_output->compression_level);

        return true;
    }

    return false;
}


}
