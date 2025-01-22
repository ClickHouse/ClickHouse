#pragma once

#include <Processors/Formats/IInputFormat.h>
#include "Interpreters/ExpressionAnalyzer.h"
#include "ManifestFile.h"
#include "Parsers/ASTFunction.h"
#include "Parsers/ASTLiteral.h"

namespace Iceberg
{

class PositionalDeleteFileInfo
{
    struct DataInterval
    {
        ssize_t start_position_in_delete_file;
        ssize_t end_position_in_delete_file;
        DataFileEntry data_file;

        bool operator<(const DataInterval & other) const { return data_file.data_file_name < other.data_file.data_file_name; }
    };

public:
    explicit PositionalDeleteFileInfo(const std::string & source_filename_) : source_filename(source_filename_) { }

    void addDataFile(const std::string & filename)
    {
        using namespace DB;

        auto context = Context{};
        auto target_path = filename;
        if (!target_path.empty() && target_path.front() != '/')
            target_path = "/" + target_path;
        DB::ASTPtr where_ast
            = makeASTFunction("equals", std::make_shared<ASTIdentifier>("file_path"), std::make_shared<ASTLiteral>(Field(target_path)));


        Block delete_block;


        auto input_format = FormatFactory::instance().getInput(
            configuration->format,
            *read_buf,
            initial_header,
            context,
            max_block_size,
            format_settings,
            need_only_count ? 1 : max_parsing_threads,
            std::nullopt,
            true /* is_remote_fs */,
            compression_method,
            need_only_count);
        auto syntax_result = TreeRewriter(context).analyze(where_ast, delete_block.back().getNamesAndTypesList());

        ExpressionAnalyzer analyzer(where_ast, syntax_result, context_);
        const std::optional<ActionsDAG> actions = analyzer.getActionsDAG(true);
        delete_format->setKeyCondition(actions, context_);
    }

    void dataFileCanBePresent(const std::string & filename)
    {
        return true;
        // DataInterval dummy_interval{-1, -1, filename};
        // auto it = known_data_intervals.lower_bound(dummy_interval);
        // if (it != known_data_intervals.end() && it->data_file.data_file_name == filename)
        //     return true;
        // return false;
    }


private:
    std::string source_filename;
    std::set<DataInterval> known_data_intervals;
};

}