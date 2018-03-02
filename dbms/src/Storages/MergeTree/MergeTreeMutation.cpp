#include <Storages/MergeTree/MergeTreeMutation.h>
#include <Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/formatAST.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterSelectQuery.h>
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
    MergeTreeData & data_, UInt32 version_, std::vector<MutationCommand> commands_)
    : data(data_), version(version_), commands(std::move(commands_))
    , log(&Logger::get(data.getLogName() + " (Mutation " + toString(version) + ")"))
{
}

static MergeTreeData::DataPartPtr getPartFromStream(IBlockInputStream & stream)
{
    if (auto part_stream = typeid_cast<const MergeTreeBlockInputStream *>(&stream))
        return part_stream->getDataPart();

    MergeTreeData::DataPartPtr ret;
    auto wrapper = [&](IBlockInputStream & child)
    {
        ret = getPartFromStream(child);
        return !!ret;
    };

    stream.forEachChild(wrapper);

    return ret;
};

void MergeTreeMutation::execute(const Context & context)
{
    /// Calculate the set of parts.
    /// TODO: Lock?
    /// TODO: add to in-progress mutations in storage
    MergeTreeData::DataPartsVector old_parts;
    {
        auto select = std::make_shared<ASTSelectQuery>();

        select->select_expression_list = std::make_shared<ASTExpressionList>();
        select->children.push_back(select->select_expression_list);
        select->select_expression_list->children.push_back(std::make_shared<ASTAsterisk>());

        auto tables = std::make_shared<ASTTablesInSelectQuery>();
        select->tables = tables;
        select->children.push_back(select->tables);
        auto tables_element = std::make_shared<ASTTablesInSelectQueryElement>();
        tables->children.push_back(tables_element);
        auto table_expr = std::make_shared<ASTTableExpression>();
        tables_element->table_expression = table_expr;
        tables_element->children.push_back(table_expr);
        auto database = std::make_shared<ASTIdentifier>(data.getDatabaseName(), ASTIdentifier::Database);
        auto table = std::make_shared<ASTIdentifier>(data.getTableName(), ASTIdentifier::Table);
        auto database_and_table = std::make_shared<ASTIdentifier>(database->name + "." + table->name, ASTIdentifier::Table);
        database_and_table->children.push_back(database);
        database_and_table->children.push_back(table);
        table_expr->database_and_table_name = database_and_table;
        table_expr->children.push_back(database_and_table);

        ASTPtr where_expression;
        if (commands.size() == 1)
            where_expression = commands[0].predicate;
        else
        {
            auto coalesced_predicates = std::make_shared<ASTFunction>();
            coalesced_predicates->name = "or";
            coalesced_predicates->arguments = std::make_shared<ASTExpressionList>();
            coalesced_predicates->children.push_back(coalesced_predicates->arguments);

            for (const MutationCommand & cmd : commands)
            {
                if (cmd.predicate)
                    coalesced_predicates->arguments->children.push_back(cmd.predicate);
            }
            where_expression = std::move(coalesced_predicates);
        }
        select->where_expression = where_expression;
        select->children.push_back(where_expression);

        auto context_copy = context;
        context_copy.getSettingsRef().merge_tree_uniform_read_distribution = 0;
        context_copy.getSettingsRef().max_threads = 1; /// Will return 1 stream per part.

        InterpreterSelectQuery interpreter_select(
            select, context_copy, {}, QueryProcessingStage::FetchColumns);
        BlockInputStreams streams = interpreter_select.executeWithMultipleStreams();

        for (const auto & stream : streams)
        {
            auto part = getPartFromStream(*stream);
            if (part)
                old_parts.push_back(part);
        }
    }

    std::vector<MutationAction> actions;

    for (const MutationCommand & cmd : commands)
    {
        LOG_TRACE(log, "MUTATION type: " << cmd.type << " predicate: " << cmd.predicate);

        if (cmd.type == MutationCommand::DELETE)
        {
            auto predicate = std::make_shared<ASTFunction>();
            predicate->name = "not";
            predicate->arguments = std::make_shared<ASTExpressionList>();
            predicate->arguments->children.push_back(cmd.predicate);
            predicate->children.push_back(predicate->arguments);

            auto predicate_expr = ExpressionAnalyzer(predicate, context, nullptr, data.getColumns().getAllPhysical())
                .getActions(false);

            String col_name = predicate->getColumnName();
            auto transform = [predicate_expr, col_name](BlockInputStreamPtr & in)
            {
                in = std::make_shared<FilterBlockInputStream>(in, predicate_expr, col_name);
            };

            actions.push_back({transform});
        }
        else
            throw Exception("Unsupported mutation cmd type: " + toString(static_cast<int>(cmd.type)),
                ErrorCodes::LOGICAL_ERROR);
    }

    MergeTreeData::Transaction txn;
    for (const auto & old_part : old_parts)
    {
        auto new_part = executeOnPart(old_part, actions, context);
        if (new_part)
            data.renameTempPartAndReplace(new_part, nullptr, &txn);
    }

    LOG_TRACE(log, "Committing " << txn.size() << " new parts.");
    txn.commit();
}

MergeTreeData::MutableDataPartPtr MergeTreeMutation::executeOnPart(
    const MergeTreeData::DataPartPtr & part,
    const std::vector<MutationAction> & actions,
    const Context & context) const
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

    for (const MutationAction & action : actions)
        action.stream_transform(in);

    auto compression_settings = context.chooseCompressionSettings(0, 0); /// TODO
    MergedBlockOutputStream out(data, new_part_tmp_path, all_columns, compression_settings);

    MergeTreeDataPart::MinMaxIndex minmax_idx;

    in->readPrefix();
    out.writePrefix();

    while (Block block = in->read())
    {
        /// We need to calculate the primary key expression to write it.
        if (data.hasPrimaryKey())
        {
            data.getPrimaryExpression()->execute(block);
            auto secondary_sort_expr = data.getSecondarySortExpression();
            if (secondary_sort_expr)
                secondary_sort_expr->execute(block);
        }

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
