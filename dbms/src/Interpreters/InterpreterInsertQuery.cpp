#include <Interpreters/InterpreterInsertQuery.h>

#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/CheckConstraintsBlockOutputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <DataStreams/InputStreamFromASTInsertQuery.h>
#include <DataStreams/NullAndDoCopyBlockInputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Storages/Kafka/StorageKafka.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/checkStackSize.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int READONLY;
    extern const int ILLEGAL_COLUMN;
    extern const int DUPLICATE_COLUMN;
}


InterpreterInsertQuery::InterpreterInsertQuery(
    const ASTPtr & query_ptr_, const Context & context_, bool allow_materialized_, bool no_squash_, bool no_destination_)
    : query_ptr(query_ptr_)
    , context(context_)
    , allow_materialized(allow_materialized_)
    , no_squash(no_squash_)
    , no_destination(no_destination_)
{
    checkStackSize();
}


StoragePtr InterpreterInsertQuery::getTable(const ASTInsertQuery & query)
{
    if (query.table_function)
    {
        const auto * table_function = query.table_function->as<ASTFunction>();
        const auto & factory = TableFunctionFactory::instance();
        TableFunctionPtr table_function_ptr = factory.get(table_function->name, context);
        return table_function_ptr->execute(query.table_function, context, table_function_ptr->getName());
    }

    /// Into what table to write.
    return context.getTable(query.database, query.table);
}

Block InterpreterInsertQuery::getSampleBlock(const ASTInsertQuery & query, const StoragePtr & table)
{
    Block table_sample_non_materialized = table->getSampleBlockNonMaterialized();
    /// If the query does not include information about columns
    if (!query.columns)
    {
        if (no_destination)
            return table->getSampleBlockWithVirtuals();
        else
            return table_sample_non_materialized;
    }

    Block table_sample = table->getSampleBlock();
    /// Form the block based on the column names from the query
    Block res;
    for (const auto & identifier : query.columns->children)
    {
        std::string current_name = identifier->getColumnName();

        /// The table does not have a column with that name
        if (!table_sample.has(current_name))
            throw Exception("No such column " + current_name + " in table " + query.table, ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!allow_materialized && !table_sample_non_materialized.has(current_name))
            throw Exception("Cannot insert column " + current_name + ", because it is MATERIALIZED column.", ErrorCodes::ILLEGAL_COLUMN);
        if (res.has(current_name))
            throw Exception("Column " + current_name + " specified more than once", ErrorCodes::DUPLICATE_COLUMN);

        res.insert(ColumnWithTypeAndName(table_sample.getByName(current_name).type, current_name));
    }
    return res;
}


BlockIO InterpreterInsertQuery::execute()
{
    const auto & query = query_ptr->as<ASTInsertQuery &>();
    checkAccess(query);
    StoragePtr table = getTable(query);

    auto table_lock = table->lockStructureForShare(true, context.getInitialQueryId());

    /// We create a pipeline of several streams, into which we will write data.
    BlockOutputStreamPtr out;

    /// NOTE: we explicitly ignore bound materialized views when inserting into Kafka Storage.
    ///       Otherwise we'll get duplicates when MV reads same rows again from Kafka.
    if (table->noPushingToViews() && !no_destination)
        out = table->write(query_ptr, context);
    else
        out = std::make_shared<PushingToViewsBlockOutputStream>(query.database, query.table, table, context, query_ptr, no_destination);

    /// Do not squash blocks if it is a sync INSERT into Distributed, since it lead to double bufferization on client and server side.
    /// Client-side bufferization might cause excessive timeouts (especially in case of big blocks).
    if (!(context.getSettingsRef().insert_distributed_sync && table->isRemote()) && !no_squash)
    {
        out = std::make_shared<SquashingBlockOutputStream>(
            out, out->getHeader(), context.getSettingsRef().min_insert_block_size_rows, context.getSettingsRef().min_insert_block_size_bytes);
    }
    auto query_sample_block = getSampleBlock(query, table);

    /// Actually we don't know structure of input blocks from query/table,
    /// because some clients break insertion protocol (columns != header)
    out = std::make_shared<AddingDefaultBlockOutputStream>(
        out, query_sample_block, out->getHeader(), table->getColumns().getDefaults(), context);

    if (const auto & constraints = table->getConstraints(); !constraints.empty())
        out = std::make_shared<CheckConstraintsBlockOutputStream>(query.table,
            out, query_sample_block, table->getConstraints(), context);

    auto out_wrapper = std::make_shared<CountingBlockOutputStream>(out);
    out_wrapper->setProcessListElement(context.getProcessListElement());
    out = std::move(out_wrapper);

    BlockIO res;
    res.out = std::move(out);

    /// What type of query: INSERT or INSERT SELECT?
    if (query.select)
    {
        /// Passing 1 as subquery_depth will disable limiting size of intermediate result.
        InterpreterSelectWithUnionQuery interpreter_select{query.select, context, SelectQueryOptions(QueryProcessingStage::Complete, 1)};

        res.in = interpreter_select.execute().in;

        res.in = std::make_shared<ConvertingBlockInputStream>(context, res.in, res.out->getHeader(), ConvertingBlockInputStream::MatchColumnsMode::Position);
        res.in = std::make_shared<NullAndDoCopyBlockInputStream>(res.in, res.out);

        res.out = nullptr;

        if (!allow_materialized)
        {
            Block in_header = res.in->getHeader();
            for (const auto & column : table->getColumns())
                if (column.default_desc.kind == ColumnDefaultKind::Materialized && in_header.has(column.name))
                    throw Exception("Cannot insert column " + column.name + ", because it is MATERIALIZED column.", ErrorCodes::ILLEGAL_COLUMN);
        }
    }
    else if (query.data && !query.has_tail) /// can execute without additional data
    {
        res.in = std::make_shared<InputStreamFromASTInsertQuery>(query_ptr, nullptr, query_sample_block, context, nullptr);
        res.in = std::make_shared<NullAndDoCopyBlockInputStream>(res.in, res.out);
        res.out = nullptr;
    }

    return res;
}


void InterpreterInsertQuery::checkAccess(const ASTInsertQuery & query)
{
    const Settings & settings = context.getSettingsRef();
    auto readonly = settings.readonly;

    if (!readonly || (query.database.empty() && context.tryGetExternalTable(query.table) && readonly >= 2))
    {
        return;
    }

    throw Exception("Cannot insert into table in readonly mode", ErrorCodes::READONLY);
}

std::pair<String, String> InterpreterInsertQuery::getDatabaseTable() const
{
    const auto & query = query_ptr->as<ASTInsertQuery &>();
    return {query.database, query.table};
}

}
