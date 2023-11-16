#pragma once

#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Poco/String.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

enum class ActionFuncType
{
    /// logical functions
    FUNC_AND,
    FUNC_OR,
    FUNC_NOT,
    /// in
    FUNC_IN,
    /// comparison functions
    FUNC_EQUAL,
    FUNC_NOT_EQUAL,
    FUNC_GREATER,
    FUNC_GREATER_OR_EQUAL,
    FUNC_LESS,
    FUNC_LESS_OR_EQUAL,
    /// others
    FUNC_OTHERS
};

static ActionFuncType getActionFuncType(const String & name)
{
    String lower_case_name = Poco::toLower(name);
    if (lower_case_name == NameAnd::name) /// TODO FUNCTION NAME
        return ActionFuncType::FUNC_AND;
    else if (lower_case_name == NameOr::name)
        return ActionFuncType::FUNC_OR;
    else if (lower_case_name == NameNot::name)
        return ActionFuncType::FUNC_NOT;
    else if (lower_case_name == "in")
        return ActionFuncType::FUNC_IN;
    else if (lower_case_name == NameEquals::name)
        return ActionFuncType::FUNC_EQUAL;
    else if (lower_case_name == NameNotEquals::name)
        return ActionFuncType::FUNC_NOT_EQUAL;
    else if (lower_case_name == NameGreater::name)
        return ActionFuncType::FUNC_GREATER;
    else if (lower_case_name == NameGreaterOrEquals::name)
        return ActionFuncType::FUNC_GREATER_OR_EQUAL;
    else if (lower_case_name == NameLess::name)
        return ActionFuncType::FUNC_LESS;
    else if (lower_case_name == NameLessOrEquals::name)
        return ActionFuncType::FUNC_LESS_OR_EQUAL;
    else
        return ActionFuncType::FUNC_OTHERS;
}

template <class R, class C>
class ActionNodeVisitor
{
public:
    using ResultType = R;
    using ContextType = C;

    virtual ~ActionNodeVisitor() = default;

    virtual R visit(const ActionsDAGPtr actions_dag_ptr, C & context);
    virtual R visit(const ActionsDAG::Node * node, C & context);

    virtual R visitChildren(const ActionsDAG::Node * node, C & context);
    virtual R visitDefault(const ActionsDAG::Node * node, C & context);

    virtual R visitInput(const ActionsDAG::Node * node, C & context);
    virtual R visitColumn(const ActionsDAG::Node * node, C & context); /// constant
    virtual R visitAlias(const ActionsDAG::Node * node, C & context);
    virtual R visitArrayJoin(const ActionsDAG::Node * node, C & context); /// change row number

    /// functions
    virtual R visitAnd(const ActionsDAG::Node * node, C & context);
    virtual R visitOr(const ActionsDAG::Node * node, C & context);
    virtual R visitNot(const ActionsDAG::Node * node, C & context);
    virtual R visitIn(const ActionsDAG::Node * node, C & context);
    virtual R visitEqual(const ActionsDAG::Node * node, C & context);
    virtual R visitNotEqual(const ActionsDAG::Node * node, C & context);
    virtual R visitGreater(const ActionsDAG::Node * node, C & context);
    virtual R visitGreaterOrEqual(const ActionsDAG::Node * node, C & context);
    virtual R visitLess(const ActionsDAG::Node * node, C & context);
    virtual R visitLessOrEqual(const ActionsDAG::Node * node, C & context);
    virtual R visitOtherFuncs(const ActionsDAG::Node * node, C & context);
};

template <class R, class C>
R ActionNodeVisitor<R, C>::visit(const ActionsDAGPtr, C &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "method not implemented");
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visit(const ActionsDAG::Node * node, C & context)
{
    R ret;
    switch (node->type)
    {
        case ActionsDAG::ActionType::INPUT:
            ret = visitInput(node, context);
            break;
        case ActionsDAG::ActionType::COLUMN:
            ret = visitColumn(node, context);
            break;
        case ActionsDAG::ActionType::ALIAS:
            ret = visitAlias(node, context);
            break;
        case ActionsDAG::ActionType::ARRAY_JOIN:
            ret = visitArrayJoin(node, context);
            break;
        case ActionsDAG::ActionType::FUNCTION:
            auto func_type = getActionFuncType(node->function_base->getName());
            switch (func_type)
            {
                case ActionFuncType::FUNC_AND:
                    ret = visitAnd(node, context);
                    break;
                case ActionFuncType::FUNC_OR:
                    ret = visitOr(node, context);
                    break;
                case ActionFuncType::FUNC_NOT:
                    ret = visitNot(node, context);
                    break;
                case ActionFuncType::FUNC_IN:
                    ret = visitIn(node, context);
                    break;
                case ActionFuncType::FUNC_EQUAL:
                    ret = visitEqual(node, context);
                    break;
                case ActionFuncType::FUNC_NOT_EQUAL:
                    ret = visitNotEqual(node, context);
                    break;
                case ActionFuncType::FUNC_GREATER:
                    ret = visitGreater(node, context);
                    break;
                case ActionFuncType::FUNC_GREATER_OR_EQUAL:
                    ret = visitGreaterOrEqual(node, context);
                    break;
                case ActionFuncType::FUNC_LESS:
                    ret = visitLess(node, context);
                    break;
                case ActionFuncType::FUNC_LESS_OR_EQUAL:
                    ret = visitLessOrEqual(node, context);
                    break;
                case ActionFuncType::FUNC_OTHERS:
                    ret = visitOtherFuncs(node, context);
                    break;
            }
            break;
    }
    return ret;
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitChildren(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitDefault(const ActionsDAG::Node *, C &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "method not implemented");
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitInput(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitColumn(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}


template <class R, class C>
R ActionNodeVisitor<R, C>::visitAlias(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitArrayJoin(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitAnd(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitOr(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitNot(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitIn(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitEqual(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitNotEqual(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitGreater(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitGreaterOrEqual(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitLess(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitLessOrEqual(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}

template <class R, class C>
R ActionNodeVisitor<R, C>::visitOtherFuncs(const ActionsDAG::Node * node, C & context)
{
    return visitDefault(node, context);
}

}
