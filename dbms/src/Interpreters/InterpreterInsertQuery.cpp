#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>

#include <Common/typeid_cast.h>

#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <DataStreams/NullAndDoCopyBlockInputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/InputStreamFromASTInsertQuery.h>
#include <DataStreams/copyData.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int READONLY;
    extern const int ILLEGAL_COLUMN;
}


InterpreterInsertQuery::InterpreterInsertQuery(
    const ASTPtr & query_ptr_, Context & context_, bool allow_materialized_)
    : query_ptr(query_ptr_), context(context_), allow_materialized(allow_materialized_)
{
}


StoragePtr InterpreterInsertQuery::getTable(const ASTInsertQuery & query)
{
    if (query.table_function)
    {
        auto table_function = typeid_cast<const ASTFunction *>(query.table_function.get());
        const auto & factory = TableFunctionFactory::instance();
        return factory.get(table_function->name, context)->execute(query.table_function, context);
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
        /// Format Native ignores header and write blocks as is.
        if (query.format == "Native")
            return {};
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

        res.insert(ColumnWithTypeAndName(table_sample.getByName(current_name).type, current_name));
    }
    return res;
}


BlockIO InterpreterInsertQuery::execute()
{
    ASTInsertQuery & query = typeid_cast<ASTInsertQuery &>(*query_ptr);
    checkAccess(query);
    StoragePtr table = getTable(query);

    auto table_lock = table->lockStructureForShare(true, context.getCurrentQueryId());

    /// We create a pipeline of several streams, into which we will write data.
    BlockOutputStreamPtr out;

    out = std::make_shared<PushingToViewsBlockOutputStream>(query.database, query.table, table, context, query_ptr, query.no_destination);

    /// Do not squash blocks if it is a sync INSERT into Distributed, since it lead to double bufferization on client and server side.
    /// Client-side bufferization might cause excessive timeouts (especially in case of big blocks).
    if (!(context.getSettingsRef().insert_distributed_sync && table->isRemote()))
    {
        out = std::make_shared<SquashingBlockOutputStream>(
            out, table->getSampleBlock(), context.getSettingsRef().min_insert_block_size_rows, context.getSettingsRef().min_insert_block_size_bytes);
    }
    auto query_sample_block = getSampleBlock(query, table);

    /// Actually we don't know structure of input blocks from query/table,
    /// because some clients break insertion protocol (columns != header)
    out = std::make_shared<AddingDefaultBlockOutputStream>(
        out, query_sample_block, table->getSampleBlock(), table->getColumns().defaults, context);

    auto out_wrapper = std::make_shared<CountingBlockOutputStream>(out);
    out_wrapper->setProcessListElement(context.getProcessListElement());
    out = std::move(out_wrapper);

    BlockIO res;
    res.out = std::move(out);

    /// What type of query: INSERT or INSERT SELECT?
    if (query.select)
    {
        /// Passing 1 as subquery_depth will disable limiting size of intermediate result.
        InterpreterSelectWithUnionQuery interpreter_select{query.select, context, {}, QueryProcessingStage::Complete, 1};

        res.in = interpreter_select.execute().in;

        res.in = std::make_shared<ConvertingBlockInputStream>(context, res.in, res.out->getHeader(), ConvertingBlockInputStream::MatchColumnsMode::Position);
        res.in = std::make_shared<NullAndDoCopyBlockInputStream>(res.in, res.out);

        res.out = nullptr;

        if (!allow_materialized)
        {
            Block in_header = res.in->getHeader();
            for (const auto & name_type : table->getColumns().materialized)
                if (in_header.has(name_type.name))
                    throw Exception("Cannot insert column " + name_type.name + ", because it is MATERIALIZED column.", ErrorCodes::ILLEGAL_COLUMN);
        }
    }
    else if (query.data && !query.has_tail) /// can execute without additional data
    {
        res.in = std::make_shared<InputStreamFromASTInsertQuery>(query_ptr, nullptr, query_sample_block, context);
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
    ASTInsertQuery & query = typeid_cast<ASTInsertQuery &>(*query_ptr);
    return {query.database, query.table};
}

}
