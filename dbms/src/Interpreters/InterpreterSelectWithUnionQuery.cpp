#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNION_ALL_RESULT_STRUCTURES_MISMATCH;
}


InterpreterSelectWithUnionQuery::InterpreterSelectWithUnionQuery(
    const ASTPtr & query_ptr_,
    const Context & context_,
    const Names & required_column_names,
    QueryProcessingStage::Enum to_stage_,
    size_t subquery_depth_)
    : query_ptr(query_ptr_),
    context(context_),
    to_stage(to_stage_),
    subquery_depth(subquery_depth_)
{
    const ASTSelectWithUnionQuery & ast = typeid_cast<const ASTSelectWithUnionQuery &>(*query_ptr);

    size_t num_selects = ast.list_of_selects->children.size();
    nested_interpreters.reserve(num_selects);

    if (!num_selects)
        throw Exception("Logical error: no children in ASTSelectWithUnionQuery", ErrorCodes::LOGICAL_ERROR);

    for (const auto & select : ast.list_of_selects->children)
        nested_interpreters.emplace_back(std::make_unique<InterpreterSelectQuery>(select, context, required_column_names, to_stage, subquery_depth));

    init();
}


InterpreterSelectWithUnionQuery::~InterpreterSelectWithUnionQuery() = default;


void InterpreterSelectWithUnionQuery::init()
{
    size_t num_selects = nested_interpreters.size();

    if (!num_selects)
        throw Exception("Logical error: no children in ASTSelectWithUnionQuery", ErrorCodes::LOGICAL_ERROR);

    if (num_selects == 1)
    {
        result_header = nested_interpreters.front()->getSampleBlock();
    }
    else
    {
        Blocks headers(num_selects);
        for (size_t query_num = 0; query_num < num_selects; ++query_num)
            headers[query_num] = nested_interpreters[query_num]->getSampleBlock();

        result_header = headers.front();
        size_t num_columns = result_header.columns();

        for (size_t query_num = 1; query_num < num_selects; ++query_num)
            if (headers[query_num].columns() != num_columns)
                throw Exception("Different number of columns in UNION ALL elements", ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH);

        for (size_t column_num = 0; column_num < num_columns; ++column_num)
        {
            ColumnWithTypeAndName & result_elem = result_header.getByPosition(column_num);

            /// Determine common type.

            DataTypes types(num_selects);
            for (size_t query_num = 0; query_num < num_selects; ++query_num)
                types[query_num] = headers[query_num].getByPosition(column_num).type;

            result_elem.type = getLeastSupertype(types);

            /// If there are different constness or different values of constants, the result must be non-constant.

            if (result_elem.column->isColumnConst())
            {
                bool need_materialize = false;
                for (size_t query_num = 1; query_num < num_selects; ++query_num)
                {
                    const ColumnWithTypeAndName & source_elem = headers[query_num].getByPosition(column_num);

                    if (!source_elem.column->isColumnConst()
                        || (static_cast<const ColumnConst &>(*result_elem.column).getField()
                            != static_cast<const ColumnConst &>(*source_elem.column).getField()))
                    {
                        need_materialize = true;
                        break;
                    }
                }

                if (need_materialize)
                    result_elem.column = result_elem.type->createColumn();
            }

            /// BTW, result column names are from first SELECT.
        }
    }
}


Block InterpreterSelectWithUnionQuery::getSampleBlock()
{
    return result_header;
}

Block InterpreterSelectWithUnionQuery::getSampleBlock(
    const ASTPtr & query_ptr,
    const Context & context)
{
    return InterpreterSelectWithUnionQuery(query_ptr, context).getSampleBlock();
}


BlockInputStreams InterpreterSelectWithUnionQuery::executeWithMultipleStreams()
{
    BlockInputStreams nested_streams;

    for (auto & interpreter : nested_interpreters)
    {
        BlockInputStreams streams = interpreter->executeWithMultipleStreams();
        nested_streams.insert(nested_streams.end(), streams.begin(), streams.end());
    }

    /// Unify data structure.
    if (nested_interpreters.size() > 1)
        for (auto & stream : nested_streams)
            stream = std::make_shared<ConvertingBlockInputStream>(context, stream, result_header, ConvertingBlockInputStream::MatchColumnsMode::Position);

    return nested_streams;
}


BlockIO InterpreterSelectWithUnionQuery::execute()
{
    BlockInputStreams nested_streams = executeWithMultipleStreams();
    BlockInputStreamPtr result_stream;

    if (nested_streams.empty())
    {
        result_stream = std::make_shared<NullBlockInputStream>(getSampleBlock());
    }
    else if (nested_streams.size() == 1)
    {
        result_stream = nested_streams.front();
        nested_streams.clear();
    }
    else
    {
        result_stream = std::make_shared<UnionBlockInputStream<>>(nested_streams, nullptr, context.getSettingsRef().max_threads);
        nested_streams.clear();
    }

    BlockIO res;
    res.in = result_stream;
    return res;
}


void InterpreterSelectWithUnionQuery::ignoreWithTotals()
{
    for (auto & interpreter : nested_interpreters)
        interpreter->ignoreWithTotals();
}

}
