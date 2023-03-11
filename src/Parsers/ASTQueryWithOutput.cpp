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

void ASTQueryWithOutput::formatImpl(const FormattingBuffer & out) const
{
    formatQueryImpl(out);

    if (out_file)
    {
        out.nlOrWs();
        out.writeKeyword("INTO OUTFILE ");
        out_file->formatImpl(out);
    }

    if (format)
    {
        out.nlOrWs();
        out.writeKeyword("FORMAT ");
        format->formatImpl(out);
    }

    if (settings_ast && assert_cast<ASTSetQuery *>(settings_ast.get())->print_in_format)
    {
        out.nlOrWs();
        out.writeKeyword("SETTINGS ");
        settings_ast->formatImpl(out);
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
