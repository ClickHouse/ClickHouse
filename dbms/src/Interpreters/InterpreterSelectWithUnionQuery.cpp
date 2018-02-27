#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
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
    const Names & required_result_column_names,
    QueryProcessingStage::Enum to_stage_,
    size_t subquery_depth_)
    : query_ptr(query_ptr_),
    context(context_),
    to_stage(to_stage_),
    subquery_depth(subquery_depth_)
{
    const ASTSelectWithUnionQuery & ast = typeid_cast<const ASTSelectWithUnionQuery &>(*query_ptr);

    size_t num_selects = ast.list_of_selects->children.size();

    if (!num_selects)
        throw Exception("Logical error: no children in ASTSelectWithUnionQuery", ErrorCodes::LOGICAL_ERROR);

    /// Check number of columns.

    size_t num_columns = 0;
    for (const auto & select : ast.list_of_selects->children)
    {
        size_t current_num_columns = typeid_cast<const ASTSelectQuery &>(*select).select_expression_list->children.size();

        if (!current_num_columns)
            throw Exception("Logical error: SELECT query has zero columns in SELECT clause", ErrorCodes::LOGICAL_ERROR);

        if (!num_columns)
            num_columns = current_num_columns;
        else if (num_columns != current_num_columns)
            throw Exception("Different number of columns in UNION ALL elements.", ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH);
    }

    /// Initialize interpreters for each SELECT query.
    /// Note that we pass 'required_result_column_names' to first SELECT.
    /// And for the rest, we pass names at the corresponding positions of 'required_result_column_names' of the first SELECT,
    ///  because names could be different.

    nested_interpreters.reserve(num_selects);

    std::vector<size_t> positions_of_required_result_columns(required_result_column_names.size());

    {
        const auto & first_select = static_cast<const ASTSelectQuery &>(*ast.list_of_selects->children.at(0));

        for (size_t required_result_num = 0, size = required_result_column_names.size(); required_result_num < size; ++required_result_num)
        {
            bool found = false;
            for (size_t position_in_select = 0; position_in_select < num_columns; ++position_in_select)
            {
                if (first_select.select_expression_list->children.at(position_in_select)->getAliasOrColumnName()
                    == required_result_column_names[required_result_num])
                {
                    found = true;
                    positions_of_required_result_columns[required_result_num] = position_in_select;
                    break;
                }
            }
            if (!found)
                throw Exception("Logical error: cannot find result column " + backQuoteIfNeed(required_result_column_names[required_result_num])
                    + " in first SELECT query in UNION ALL", ErrorCodes::LOGICAL_ERROR);
        }
    }

    for (size_t query_num = 0; query_num < num_selects; ++query_num)
    {
        const auto & select = ast.list_of_selects->children.at(query_num);
        Names current_required_result_column_names(required_result_column_names.size());
        for (size_t required_result_num = 0, size = required_result_column_names.size(); required_result_num < size; ++required_result_num)
            current_required_result_column_names[required_result_num]
                = static_cast<const ASTSelectQuery &>(*select).select_expression_list
                    ->children.at(positions_of_required_result_columns[required_result_num])->getAliasOrColumnName();

        nested_interpreters.emplace_back(std::make_unique<InterpreterSelectQuery>(select, context, current_required_result_column_names, to_stage, subquery_depth));
    }

    /// Determine structure of result.

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


InterpreterSelectWithUnionQuery::~InterpreterSelectWithUnionQuery() = default;


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
