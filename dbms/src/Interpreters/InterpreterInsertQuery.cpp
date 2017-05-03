#include <IO/ConcatReadBuffer.h>

#include <DataStreams/ProhibitColumnsBlockOutputStream.h>
#include <DataStreams/MaterializingBlockOutputStream.h>
#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/NullAndDoCopyBlockInputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <DataStreams/NullableAdapterBlockInputStream.h>
#include <DataStreams/CastTypeBlockInputStream.h>
#include <DataStreams/copyData.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>

#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>


namespace ProfileEvents
{
    extern const Event InsertQuery;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}


InterpreterInsertQuery::InterpreterInsertQuery(ASTPtr query_ptr_, Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
    ProfileEvents::increment(ProfileEvents::InsertQuery);
}


StoragePtr InterpreterInsertQuery::getTable()
{
    ASTInsertQuery & query = typeid_cast<ASTInsertQuery &>(*query_ptr);

    /// In what table to write.
    return context.getTable(query.database, query.table);
}

Block InterpreterInsertQuery::getSampleBlock()
{
    ASTInsertQuery & query = typeid_cast<ASTInsertQuery &>(*query_ptr);

    /// If the query does not include information about columns
    if (!query.columns)
        return getTable()->getSampleBlockNonMaterialized();

    Block table_sample = getTable()->getSampleBlock();

    /// Form the block based on the column names from the query
    Block res;
    for (const auto & identifier : query.columns->children)
    {
        std::string current_name = identifier->getColumnName();

        /// The table does not have a column with that name
        if (!table_sample.has(current_name))
            throw Exception("No such column " + current_name + " in table " + query.table, ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        ColumnWithTypeAndName col;
        col.name = current_name;
        col.type = table_sample.getByName(current_name).type;
        col.column = col.type->createColumn();
        res.insert(std::move(col));
    }

    return res;
}


BlockIO InterpreterInsertQuery::execute()
{
    ASTInsertQuery & query = typeid_cast<ASTInsertQuery &>(*query_ptr);
    StoragePtr table = getTable();

    auto table_lock = table->lockStructure(true);

    NamesAndTypesListPtr required_columns = std::make_shared<NamesAndTypesList>(table->getColumnsList());

    /// We create a pipeline of several streams, into which we will write data.
    BlockOutputStreamPtr out;

    out = std::make_shared<PushingToViewsBlockOutputStream>(query.database, query.table, context, query_ptr);

    out = std::make_shared<MaterializingBlockOutputStream>(out);

    out = std::make_shared<AddingDefaultBlockOutputStream>(out,
        required_columns, table->column_defaults, context, static_cast<bool>(context.getSettingsRef().strict_insert_defaults));

    out = std::make_shared<ProhibitColumnsBlockOutputStream>(out, table->materialized_columns);

    out = std::make_shared<SquashingBlockOutputStream>(out,
        context.getSettingsRef().min_insert_block_size_rows,
        context.getSettingsRef().min_insert_block_size_bytes);

    auto out_wrapper = std::make_shared<CountingBlockOutputStream>(out);
    out_wrapper->setProcessListElement(context.getProcessListElement());
    out = std::move(out_wrapper);

    BlockIO res;
    res.out_sample = getSampleBlock();

    /// What type of query: INSERT or INSERT SELECT?
    if (!query.select)
    {
        res.out = out;
    }
    else
    {
        InterpreterSelectQuery interpreter_select{query.select, context};
        res.in_sample = interpreter_select.getSampleBlock();

        res.in = interpreter_select.execute().in;

        res.in = std::make_shared<NullableAdapterBlockInputStream>(res.in, res.in_sample, res.out_sample);
        res.in = std::make_shared<CastTypeBlockInputStream>(context, res.in, res.in_sample, res.out_sample);
        res.in = std::make_shared<NullAndDoCopyBlockInputStream>(res.in, out);
    }

    return res;
}


}
