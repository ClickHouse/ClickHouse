#include <Storages/MergeTree/MergeTreeMutation.h>
#include <Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/formatAST.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/copyData.h>

#include <Poco/File.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeMutation::MergeTreeMutation(
    MergeTreeData & data_, Int64 version_, std::vector<MutationCommand> commands_)
    : data(data_), version(version_), commands(std::move(commands_))
    , log(&Logger::get(data.getLogName() + " (Mutation " + toString(version) + ")"))
{
    for (const auto & cmd : commands)
    {
        LOG_TRACE(log, "MUTATION type: " << cmd.type << " predicate: " << cmd.predicate);
    }
}

MergeTreeData::MutableDataPartPtr MergeTreeMutation::executeOnPart(const MergeTreeData::DataPartPtr & part, const Context & context) const
{
    LOG_TRACE(log, "Executing on part " << part->name);

    MergeTreePartInfo new_part_info = part->info;
    new_part_info.version = version;

    String new_part_name;
    if (data.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        new_part_name = new_part_info.getPartNameV0(part->getMinDate(), part->getMaxDate());
    else
        new_part_name = new_part_info.getPartName();

    MergeTreeData::MutableDataPartPtr new_data_part = std::make_shared<MergeTreeData::DataPart>(
        data, new_part_name, new_part_info);
    new_data_part->relative_path = "tmp_mut_" + new_part_name;
    new_data_part->is_temp = true;

    String new_part_tmp_path = new_data_part->getFullPath();

    Poco::File(new_part_tmp_path).createDirectories();

    NamesAndTypesList all_columns = data.getColumns().getAllPhysical();

    BlockInputStreamPtr in = std::make_shared<MergeTreeBlockInputStream>(
        data, part, DEFAULT_MERGE_BLOCK_SIZE, 0, 0, all_columns.getNames(),
        MarkRanges(1, MarkRange(0, part->marks_count)),
        false, nullptr, String(), true, 0, DBMS_DEFAULT_BUFFER_SIZE, false);

    for (const MutationCommand & cmd : commands)
    {
        if (cmd.type == MutationCommand::DELETE)
        {
            /// TODO: maybe there is a better way to negate a predicate for DELETE.
            auto predicate = std::make_shared<ASTFunction>();
            predicate->name = "not";
            predicate->arguments = std::make_shared<ASTExpressionList>();
            predicate->arguments->children.push_back(cmd.predicate);
            predicate->children.push_back(predicate->arguments);

            auto predicate_expr = ExpressionAnalyzer(predicate, context, nullptr, all_columns)
                .getActions(false);
            String col_name = predicate->getColumnName();

            in = std::make_shared<FilterBlockInputStream>(in, predicate_expr, col_name);
        }
        else
            throw Exception("Unsupported mutation cmd type: " + toString(static_cast<int>(cmd.type)),
                ErrorCodes::LOGICAL_ERROR);
    }

    auto compression_settings = context.chooseCompressionSettings(0, 0); /// TODO
    MergedBlockOutputStream out(data, new_part_tmp_path, all_columns, compression_settings);

    MergeTreeDataPart::MinMaxIndex minmax_idx;

    in->readPrefix();
    out.writePrefix();

    while (Block block = in->read())
    {
        minmax_idx.update(block, data.minmax_idx_columns);
        out.write(block);
    }

    new_data_part->partition.assign(part->partition);
    new_data_part->minmax_idx = std::move(minmax_idx);

    in->readSuffix();
    out.writeSuffixAndFinalizePart(new_data_part);

    return new_data_part;
}

}
