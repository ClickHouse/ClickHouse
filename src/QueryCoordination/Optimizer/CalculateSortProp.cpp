#include <Functions/IFunction.h>
#include <QueryCoordination/Optimizer/CalculateSortProp.h>

namespace DB
{

CalculateSortProp::CalculateSortProp(const ActionsDAGPtr & actions_, const SortProperty & input_sort_property_)
    : actions(actions_), input_sort_property(input_sort_property_)
{
}

SortProperty CalculateSortProp::calcSortProp()
{
    SortProperty res;
    for (size_t i = 0; i < input_sort_property.size(); ++i)
    {
        const auto & one_res = calcOneSortProp(input_sort_property[i]);
        res.insert(res.end(), one_res.begin(), one_res.end());
    }
    return res;
}

SortProperty CalculateSortProp::calcOneSortProp(const SortDesc & input_sort_description)
{
    if (input_sort_description.empty())
        return {};

    std::unordered_map<String, size_t> input_sort_column_index;
    for (size_t i = 0; i < input_sort_description.size(); ++i)
        input_sort_column_index.emplace(input_sort_description[i].column_name, i);

    /// One input may have mutil output. e.g: select toStartOfMonth(dt),dt from aaa_all order by dt
    std::unordered_map<size_t, std::vector<std::pair<bool, SortColumnDesc>>> input_sort_index_output;
    const auto & actions_outputs = actions->getOutputs();
    for (const auto * output_node : actions_outputs)
    {
        MonotonicityState state;
        auto res = tryFindInputSortColumn(output_node, state, input_sort_column_index, input_sort_description);
        if (res)
        {
            input_sort_index_output[res.column_index].emplace_back(
                res.need_is_last_key,
                SortColumnDesc{output_node->result_name, res.direction, input_sort_description[res.column_index].nulls_direction});
        }
    }

    /// Look for input_sort_description in order. e.g: select a,c from aaa_all order by a,b,c. Because input_sort_description's second column b is not in output, so output is order by a.
    /// E.g: select a/2, a, b/2, b from aaa_all order by a,b
    std::vector<std::vector<std::pair<bool, SortColumnDesc>>> sort_prop_tmp;

    /// input_sort_description is [a, b]
    for (size_t i = 0; i < input_sort_description.size(); ++i)
    {
        /// input_sort_index_output is [ (0, [(false, a/2), (false, a)]), (1, [(false, b/2), (false, b)]) ]
        if (!input_sort_index_output.contains(i))
            break;

        /// output_sort_columns is [(false, a/2), (false, a)]
        auto & output_sort_columns = input_sort_index_output.at(i);
        if (i == 0)
        {
            /// need add new
            /// sort_prop_tmp is [ [(false, a/2)], [(false, a)] ]
            sort_prop_tmp.emplace_back(output_sort_columns);
        }
        else
        {
            /// sort_prop_tmp is [ [(false, a/2)], [(false, a)] ]
            for (auto & pre_sort_columns : sort_prop_tmp)
            {
                /// pre_sort_columns is [(false, a/2)]
                if (!pre_sort_columns.back().first)
                {
                    auto origin_pre_sort_columns = pre_sort_columns;

                    /// output_sort_columns is [(false, b/2), (false, b)]
                    for (size_t j = 0; j < output_sort_columns.size(); ++j)
                    {
                        auto & [last_key, sort_column] = output_sort_columns[j];
                        if (j == 0)
                        {
                            /// pre_sort_columns is [(false, a/2), (false, b/2)]
                            pre_sort_columns.emplace_back(last_key, sort_column);
                        }
                        else
                        {
                            /// need add new
                            std::vector<std::pair<bool, SortColumnDesc>> sort_description = origin_pre_sort_columns;

                            /// sort_description is [(false, a/2), (false, b)]
                            sort_description.emplace_back(last_key, sort_column);

                            /// sort_prop_tmp is [ [(false, a/2), (false, b/2)], [(false, a/2), (false, b)] ]
                            sort_prop_tmp.emplace_back(sort_description);
                        }
                    }
                }
            }
        }
    }

    SortProperty sort_property;
    /// sort_prop_tmp is [ [(false, a/2), (false, b/2)], [(false, a/2), (false, b)], [(false, a), (false, b/2)], [(false, a), (false, b)] ]
    for (auto & sort_columns : sort_prop_tmp)
    {
        SortDesc sort_description;

        /// sort_columns is [(false, a/2), (false, b/2)]
        for (auto & [_, sort_column] : sort_columns)
        {
            sort_description.emplace_back(sort_column);
        }

        /// sort_description is [(a/2), (b/2)]
        sort_property.emplace_back(sort_description);
    }

    /// sort_property is [ [(a/2), (b/2)], [(a/2), (b)], [(a), (b/2)], [(a), (b)] ]
    return sort_property;
}

/*
E.g 1:
select id/2 from aaa_all where id>1 order by id

output:    id/2
              \
input:         id

select id/2,id from aaa_all where id>1 order by id

output:    id/2   id
              \  /
input:         id

id/2 is monotonicity

input id:   2,4,6,8
output id/2, id: (1, 2), (2, 4), (3, 6), (4,8)


E.g 2:
select toStartOfMonth(dt),id from aaa_all order by dt,id

output: toStartOfMonth(dt)           id
                         \             \
input:                    dt            id

select toStartOfMonth(dt),dt,id from aaa_all order by dt,id

output: toStartOfMonth(dt)   dt      id
                         \  /          \
input:                    dt            id

input dt,id -- ('2020-01-01', 1), ('2020-01-02', 0), ('2020-01-03', 1)
output toStartOfMonth(dt),id -- ('2020-01-01', 1), ('2020-01-01', 0), ('2020-01-01', 1)
output toStartOfMonth(dt),dt,id -- ('2020-01-01', '2020-01-01', 1), ('2020-01-01', '2020-01-02', 0), ('2020-01-01', '2020-01-03', 1)

if output is "toStartOfMonth(dt),dt,id", we have two sort description: toStartOfMonth(dt) and dt,id


E.g 3:
select a/2, a, b/2, b from aaa_all order by a,b

output:  a/2   a     b/2   b
           \  /        \  /
input:      a           b

input a,b -- (1000, 2), (2000, 2), (2000, 4), (3000, 6)
output a/2, a, b/2, b -- (500, 1000, 1, 2), (1000, 2000, 1, 2), (1000, 2000, 2, 4), (1500, 3000, 3, 6)

output is "a/2, a, b/2, b", we have four sort description: (a, b), (a, b/2), (a/2, b), (a/2, b/2)
*/


CalculateSortProp::FindResult CalculateSortProp::tryFindInputSortColumn(
    const ActionsDAG::Node * node,
    MonotonicityState & last_state,
    std::unordered_map<String, size_t> & input_sort_column_index,
    const SortDesc & input_sort_description)
{
    if (!node)
        return {};

    if (node->type == ActionsDAG::ActionType::INPUT)
    {
        if (input_sort_column_index.contains(node->result_name))
        {
            size_t index = input_sort_column_index.at(node->result_name);
            if (last_state)
            {
                return {index, last_state.direction * input_sort_description[index].direction, last_state.need_is_last_key};
            }
            else /// maybe only ALIAS
            {
                return {index, input_sort_description[index].direction, false};
            }
        }
    }

    if (node->type == ActionsDAG::ActionType::FUNCTION)
    {
        const auto & func = node->function_base;
        if (!func->hasInformationAboutMonotonicity())
            return {};

        auto monotonicity = func->getMonotonicityForRange(*func->getArgumentTypes().at(0), {}, {});
        if (!monotonicity.is_monotonic)
            return {};

        if (!monotonicity.is_strict)
            last_state.need_is_last_key = true;

        if (last_state)
        {
            if (!monotonicity.is_positive)
                last_state.direction *= -1;
        }
        else
        {
            if (!monotonicity.is_positive)
                last_state.direction = -1;
            else
                last_state.direction = 1;
        }

        return tryFindInputSortColumn(node->children.front(), last_state, input_sort_column_index, input_sort_description);
    }
    else if (node->type == ActionsDAG::ActionType::ALIAS)
    {
        return tryFindInputSortColumn(node->children.front(), last_state, input_sort_column_index, input_sort_description);
    }
    else
        return {};
}

}
