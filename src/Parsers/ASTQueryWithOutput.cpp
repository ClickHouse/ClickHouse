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
    if (format)
    {
        cloned.format = format->clone();
        cloned.children.push_back(cloned.format);
    }
    if (settings_ast)
    {
        cloned.settings_ast = settings_ast->clone();
        cloned.children.push_back(cloned.settings_ast);
    }
}

void ASTQueryWithOutput::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    formatQueryImpl(s, state, frame);

    std::string indent_str = s.isOneLine() ? "" : std::string(4u * frame.indent, ' ');

    if (out_file)
    {
        s.nlOrWs();
        s.writeKeyword("INTO OUTFILE ");
        out_file->formatImpl(s, state, frame);
    }

    if (format)
    {
        s.nlOrWs();
        s.writeKeyword("FORMAT ");
        format->formatImpl(s, state, frame);
    }

    if (settings_ast && assert_cast<ASTSetQuery *>(settings_ast.get())->print_in_format)
    {
        s.nlOrWs();
        s.writeKeyword("SETTINGS ");
        settings_ast->formatImpl(s, state, frame);
    }
}

bool ASTQueryWithOutput::resetOutputASTIfExist(IAST & ast)
{
    /// FIXME: try to prettify this cast using `as<>()`
    if (auto * ast_with_output = dynamic_cast<ASTQueryWithOutput *>(&ast))
    {
        ast_with_output->format.reset();
        ast_with_output->out_file.reset();
        ast_with_output->settings_ast.reset();
        return true;
    }

    return false;
}


}
