#include <Analyzer/Passes/DictGetTupleElementPass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

#include <DataTypes/DataTypeTuple.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_dictget_tuple_element;
}

namespace
{

/** Extract the element index (0-based) from a tupleElement call.
  * Supports both positional (1-based UInt64) and named (String) access.
  * Returns std::nullopt if not determinable at analysis time.
  */
std::optional<size_t> getTupleElementIndex(const ConstantNode & index_node, const Strings & attribute_names)
{
    Field value = index_node.getValue();

    if (value.getType() == Field::Types::String)
    {
        /// Named access: dictGet returns a named tuple, so `.country` becomes tupleElement(..., 'country')
        const auto & name = value.safeGet<String>();
        for (size_t i = 0; i < attribute_names.size(); ++i)
        {
            if (attribute_names[i] == name)
                return i;
        }
        return std::nullopt;
    }

    if (value.getType() == Field::Types::UInt64)
    {
        UInt64 idx = value.safeGet<UInt64>();
        if (idx >= 1 && idx <= attribute_names.size())
            return static_cast<size_t>(idx - 1);
    }

    return std::nullopt;
}

/** Try to extract attribute names from the second argument of dictGet/dictGetOrDefault.
  * Returns empty vector if not a constant tuple of strings.
  */
Strings extractAttributeNames(const QueryTreeNodePtr & arg)
{
    Strings result;

    if (const auto * constant_node = arg->as<ConstantNode>())
    {
        Field value = constant_node->getValue();

        if (value.getType() == Field::Types::Tuple)
        {
            const auto & tuple = value.safeGet<Tuple>();
            for (const auto & elem : tuple)
            {
                if (elem.getType() != Field::Types::String)
                    return {};
                result.push_back(elem.safeGet<String>());
            }
            return result;
        }
    }
    else if (const auto * function_node = arg->as<FunctionNode>())
    {
        if (function_node->getFunctionName() == "tuple")
        {
            for (const auto & child : function_node->getArguments().getNodes())
            {
                const auto * child_constant = child->as<ConstantNode>();
                if (!child_constant)
                    return {};

                Field value = child_constant->getValue();
                if (value.getType() != Field::Types::String)
                    return {};

                result.push_back(value.safeGet<String>());
            }
            return result;
        }
    }

    return {};
}


class DictGetTupleElementVisitor : public InDepthQueryTreeVisitorWithContext<DictGetTupleElementVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<DictGetTupleElementVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_dictget_tuple_element])
            return;

        auto * tuple_element_function = node->as<FunctionNode>();
        if (!tuple_element_function || tuple_element_function->getFunctionName() != "tupleElement")
            return;

        auto & tuple_element_args = tuple_element_function->getArguments().getNodes();
        if (tuple_element_args.size() != 2)
            return;

        auto * dict_get_function = tuple_element_args[0]->as<FunctionNode>();
        if (!dict_get_function)
            return;

        const auto & dict_get_name = dict_get_function->getFunctionName();
        bool is_dict_get = (dict_get_name == "dictGet");
        bool is_dict_get_or_default = (dict_get_name == "dictGetOrDefault");

        if (!is_dict_get && !is_dict_get_or_default)
            return;

        auto & dict_get_args = dict_get_function->getArguments().getNodes();

        /// dictGet has at least 3 args: dict_name, attr_names, key_expr [, ...]
        /// dictGetOrDefault has at least 4: dict_name, attr_names, key_expr, default_value [, ...]
        if (dict_get_args.size() < 3)
            return;

        /// Extract attribute names from the second argument
        Strings attribute_names = extractAttributeNames(dict_get_args[1]);
        if (attribute_names.size() < 2)
            return;

        /// Get the tuple element index
        const auto * index_node = tuple_element_args[1]->as<ConstantNode>();
        if (!index_node)
            return;

        auto maybe_index = getTupleElementIndex(*index_node, attribute_names);
        if (!maybe_index)
            return;

        size_t element_index = *maybe_index;

        /// Prepare the new attribute name argument
        auto new_attr_arg = std::make_shared<ConstantNode>(attribute_names[element_index]);

        /// For dictGetOrDefault, prepare the new default value argument before mutating anything.
        /// If we can't extract it, bail out without touching the tree.
        QueryTreeNodePtr new_default_arg;
        if (is_dict_get_or_default)
        {
            size_t default_arg_idx = dict_get_args.size() - 1;
            const auto & default_arg = dict_get_args[default_arg_idx];

            /// `dictGetOrDefault` casts the entire default tuple to the dict's attribute tuple type at
            /// execution (see `castColumnAccurate` in `FunctionDictGetNoType::executeImpl`). If the
            /// element types of the default tuple don't match the dict's attribute types, that cast
            /// can throw on an un-selected element (for example, a string default for a `UInt64`
            /// attribute). After rewriting to a single-attribute call only the selected element is
            /// cast, so such exceptions are suppressed. Bail out unless element types match exactly
            /// (names may differ — they don't affect cast semantics).
            const auto * default_tuple_type = typeid_cast<const DataTypeTuple *>(default_arg->getResultType().get());
            const auto * result_tuple_type = typeid_cast<const DataTypeTuple *>(dict_get_function->getResultType().get());
            if (!default_tuple_type || !result_tuple_type)
                return;

            const auto & default_elems = default_tuple_type->getElements();
            const auto & result_elems = result_tuple_type->getElements();
            if (default_elems.size() != result_elems.size())
                return;
            for (size_t i = 0; i < default_elems.size(); ++i)
                if (!default_elems[i]->equals(*result_elems[i]))
                    return;

            if (const auto * default_constant = default_arg->as<ConstantNode>())
            {
                Field default_value = default_constant->getValue();
                if (default_value.getType() == Field::Types::Tuple)
                {
                    const auto & default_tuple = default_value.safeGet<Tuple>();
                    if (element_index < default_tuple.size())
                        new_default_arg = std::make_shared<ConstantNode>(default_tuple[element_index]);
                    else
                        return; /// Cannot optimize — index out of range
                }
            }
            else if (const auto * default_function = default_arg->as<FunctionNode>())
            {
                if (default_function->getFunctionName() == "tuple")
                {
                    const auto & default_tuple_args = default_function->getArguments().getNodes();
                    if (element_index >= default_tuple_args.size())
                        return; /// Cannot optimize — index out of range

                    /// Only rewrite when all tuple arguments are constants. Otherwise, dropping the
                    /// other elements could lose side effects (e.g. `tuple('ok', throwIf(1))` would
                    /// no longer evaluate `throwIf(1)` after the rewrite, changing query semantics).
                    bool all_constants = true;
                    for (const auto & tuple_arg : default_tuple_args)
                    {
                        if (!tuple_arg->as<ConstantNode>())
                        {
                            all_constants = false;
                            break;
                        }
                    }

                    if (all_constants)
                        new_default_arg = default_tuple_args[element_index];
                }
            }
        }

        /// For dictGetOrDefault, both pieces must be rewritable — bail out if not
        if (is_dict_get_or_default && !new_default_arg)
            return;

        /// The outer tupleElement node (and hence its child dictGet) may be shared across
        /// multiple parents — e.g. ORDER BY ALL referencing SELECT expressions. Mutating
        /// dict_get_function in place would change its return type from Tuple to a scalar,
        /// leaving the other parent's tupleElement wrapping a now-scalar dictGet. Clone
        /// the dictGet so the original remains intact for any other parents.
        auto new_dict_get_node = dict_get_function->clone();
        auto & new_dict_get_function = new_dict_get_node->as<FunctionNode &>();
        auto & new_dict_get_args = new_dict_get_function.getArguments().getNodes();

        new_dict_get_args[1] = std::move(new_attr_arg);

        if (new_default_arg)
        {
            size_t default_arg_idx = new_dict_get_args.size() - 1;
            new_dict_get_args[default_arg_idx] = std::move(new_default_arg);
        }

        resolveOrdinaryFunctionNodeByName(new_dict_get_function, dict_get_name, getContext());

        /// The single-attribute form of `dictGet` preserves a `LowCardinality` wrapper from the
        /// key argument, while the tuple-attribute form drops it (a tuple cannot be `LowCardinality`).
        /// If the result types differ, the rewrite would change the type observed by the parent
        /// node and break the surrounding query tree. Bail out in that case.
        if (!new_dict_get_function.getResultType()->equals(*tuple_element_function->getResultType()))
            return;

        node = std::move(new_dict_get_node);
    }
};

}

void DictGetTupleElementPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    DictGetTupleElementVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
