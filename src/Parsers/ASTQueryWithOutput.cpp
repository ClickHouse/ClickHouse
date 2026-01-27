#include <Parsers/ASTQueryWithOutput.h>

#include <Parsers/ASTSetQuery.h>


namespace DB
{

void ASTQueryWithOutput::cloneOutputOptions(ASTQueryWithOutput & cloned) const
{
    /// Reset indices first since children was cleared
    cloned.resetOutputIndices();

    if (auto out_file = getOutFile())
        cloned.setOutFile(out_file->clone());
    if (auto format_ast = getFormatAst())
        cloned.setFormatAst(format_ast->clone());
    if (auto settings_ast = getSettingsAst())
        cloned.setSettingsAst(settings_ast->clone());
    if (auto compression = getCompression())
        cloned.setCompression(compression->clone());
    if (auto compression_level = getCompressionLevel())
        cloned.setCompressionLevel(compression_level->clone());
}

void ASTQueryWithOutput::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    formatQueryImpl(ostr, s, state, frame);

    std::string indent_str = s.one_line ? "" : std::string(4u * frame.indent, ' ');

    if (auto out_file = getOutFile())
    {
        ostr << s.nl_or_ws << indent_str << "INTO OUTFILE ";
        out_file->format(ostr, s, state, frame);

        if (isOutfileAppend())
            ostr << " APPEND";
        if (isOutfileTruncate())
            ostr << " TRUNCATE";
        if (isIntoOutfileWithStdout())
            ostr << " AND STDOUT";
        if (auto compression = getCompression())
        {
            ostr << " COMPRESSION ";
            compression->format(ostr, s, state, frame);
        }
        if (auto compression_level = getCompressionLevel())
        {
            ostr << indent_str << " LEVEL ";
            compression_level->format(ostr, s, state, frame);
        }
    }

    if (auto format_ast = getFormatAst())
    {
        ostr << s.nl_or_ws << indent_str << "FORMAT ";
        format_ast->format(ostr, s, state, frame);
    }

    if (auto settings_ast = getSettingsAst())
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
        /// Collect valid indices and sort in descending order to remove from the end first,
        /// so that earlier indices remain valid during removal.
        std::vector<UInt8> indices_to_remove;
        if (ast_with_output->out_file_index != INVALID_INDEX)
            indices_to_remove.push_back(ast_with_output->out_file_index);
        if (ast_with_output->format_ast_index != INVALID_INDEX)
            indices_to_remove.push_back(ast_with_output->format_ast_index);
        if (ast_with_output->settings_ast_index != INVALID_INDEX)
            indices_to_remove.push_back(ast_with_output->settings_ast_index);
        if (ast_with_output->compression_index != INVALID_INDEX)
            indices_to_remove.push_back(ast_with_output->compression_index);
        if (ast_with_output->compression_level_index != INVALID_INDEX)
            indices_to_remove.push_back(ast_with_output->compression_level_index);

        std::sort(indices_to_remove.begin(), indices_to_remove.end(), std::greater());

        for (UInt8 idx : indices_to_remove)
            ast_with_output->children.erase(ast_with_output->children.begin() + idx);

        ast_with_output->out_file_index = INVALID_INDEX;
        ast_with_output->format_ast_index = INVALID_INDEX;
        ast_with_output->settings_ast_index = INVALID_INDEX;
        ast_with_output->compression_index = INVALID_INDEX;
        ast_with_output->compression_level_index = INVALID_INDEX;
        return true;
    }

    return false;
}

bool ASTQueryWithOutput::hasOutputOptions() const
{
    return getOutFile() || getFormatAst() || getSettingsAst() || getCompression() || getCompressionLevel();
}

}
