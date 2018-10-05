#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <Parsers/queryToString.h>


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
    size_t subquery_depth_,
    bool only_analyze)
    : query_ptr(query_ptr_),
    context(context_),
    to_stage(to_stage_),
    subquery_depth(subquery_depth_)
{
    const ASTSelectWithUnionQuery & ast = typeid_cast<const ASTSelectWithUnionQuery &>(*query_ptr);

    size_t num_selects = ast.list_of_selects->children.size();

    if (!num_selects)
        throw Exception("Logical error: no children in ASTSelectWithUnionQuery", ErrorCodes::LOGICAL_ERROR);

    /// Initialize interpreters for each SELECT query.
    /// Note that we pass 'required_result_column_names' to first SELECT.
    /// And for the rest, we pass names at the corresponding positions of 'required_result_column_names' in the result of first SELECT,
    ///  because names could be different.

    nested_interpreters.reserve(num_selects);

    std::vector<Names> required_result_column_names_for_other_selects(num_selects);
    if (!required_result_column_names.empty() && num_selects > 1)
    {
        /// Result header if there are no filtering by 'required_result_column_names'.
        /// We use it to determine positions of 'required_result_column_names' in SELECT clause.

        Block full_result_header = InterpreterSelectQuery(
            ast.list_of_selects->children.at(0), context, Names(), to_stage, subquery_depth, true).getSampleBlock();

        std::vector<size_t> positions_of_required_result_columns(required_result_column_names.size());
        for (size_t required_result_num = 0, size = required_result_column_names.size(); required_result_num < size; ++required_result_num)
            positions_of_required_result_columns[required_result_num] = full_result_header.getPositionByName(required_result_column_names[required_result_num]);

        for (size_t query_num = 1; query_num < num_selects; ++query_num)
        {
            Block full_result_header_for_current_select = InterpreterSelectQuery(
                ast.list_of_selects->children.at(query_num), context, Names(), to_stage, subquery_depth, true).getSampleBlock();

            if (full_result_header_for_current_select.columns() != full_result_header.columns())
                throw Exception("Different number of columns in UNION ALL elements:\n"
                    + full_result_header.dumpNames()
                    + "\nand\n"
                    + full_result_header_for_current_select.dumpNames() + "\n",
                    ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH);

            required_result_column_names_for_other_selects[query_num].reserve(required_result_column_names.size());
            for (const auto & pos : positions_of_required_result_columns)
                required_result_column_names_for_other_selects[query_num].push_back(full_result_header_for_current_select.getByPosition(pos).name);
        }
    }

    for (size_t query_num = 0; query_num < num_selects; ++query_num)
    {
        const Names & current_required_result_column_names = query_num == 0
            ? required_result_column_names
            : required_result_column_names_for_other_selects[query_num];

        nested_interpreters.emplace_back(std::make_unique<InterpreterSelectQuery>(
            ast.list_of_selects->children.at(query_num), context, current_required_result_column_names, to_stage, subquery_depth, only_analyze));
    }

    /// Determine structure of the result.

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
                throw Exception("Different number of columns in UNION ALL elements:\n"
                    + result_header.dumpNames()
                    + "\nand\n"
                    + headers[query_num].dumpNames() + "\n",
                    ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH);

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
    auto & cache = context.getSampleBlockCache();
    /// Using query string because query_ptr changes for every internal SELECT
    auto key = queryToString(query_ptr);
    if (cache.find(key) != cache.end())
    {
        return cache[key];
    }

    return cache[key] = InterpreterSelectWithUnionQuery(query_ptr, context, {}, QueryProcessingStage::Complete, 0, true).getSampleBlock();
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
    const Settings & settings = context.getSettingsRef();

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
        result_stream = std::make_shared<UnionBlockInputStream<>>(nested_streams, nullptr, settings.max_threads);
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
