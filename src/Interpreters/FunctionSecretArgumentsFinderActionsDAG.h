#pragma once

#include <Columns/ColumnConst.h>
#include <Common/FieldVisitorToString.h>
#include <Core/Field.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Parsers/FunctionSecretArgumentsFinder.h>


namespace DB
{

  /// This class lets `FunctionSecretArgumentsFinder` read a FUNCTION node of an `ActionsDAG`.
  /// It is the sibling of `FunctionAST` (parser AST) and `FunctionTreeNodeImpl` (analyzer query tree).
  /// Those two are used by `FunctionSecretArgumentsFinderAST` and `FunctionSecretArgumentsFinderTreeNodeImpl`.
  /// The finder code is written one time against `AbstractFunction`. This class answers its four
  /// questions about an argument for each DAG node type:
  ///
  ///   - `COLUMN`, or a `FUNCTION` that was folded to a constant: this is a literal.
  ///     `tryGetConstantField` reads the `ColumnConst` from `column`. It does not look at the node type.
  ///     `tryGetString` returns the value of a `String` constant. `tryGetLiteralText` returns the SQL
  ///     text of any scalar constant, with quotes for strings.
  ///   - `FUNCTION` that was not folded: this is a nested call, for example `equals(key, value)`.
  ///     `getFunction` wraps it, so the finder can look at its children.
  ///   - `INPUT`: this is a column reference. It is the DAG form of an identifier. `isIdentifier` is true.
  ///     `tryGetString` returns the column name, but only when the caller allows identifiers.
  ///     A DAG never holds a named collection or a table function, so an `INPUT` is always a column.
  ///   - `ALIAS`: the constructor removes it with `unwrapAlias`. The finder sees the child instead.
  ///   - `ARRAY_JOIN`, `PLACEHOLDER`: every question answers false. The finder then hides the argument.
class FunctionActionsDAG : public AbstractFunction
{
public:
    class ArgumentActionsDAG : public Argument
    {
    public:
        explicit ArgumentActionsDAG(const ActionsDAG::Node * argument_) : argument(unwrapAlias(argument_)) {}
        std::unique_ptr<AbstractFunction> getFunction() const override
        {
            if (argument->type == ActionsDAG::ActionType::FUNCTION)
                return std::make_unique<FunctionActionsDAG>(*argument);
            return nullptr;
        }
        bool isIdentifier() const override { return argument->type == ActionsDAG::ActionType::INPUT; }
        bool tryGetString(String * res, bool allow_identifier) const override
        {
            Field field;
            if (tryGetConstantField(field))
            {
                if (field.getType() != Field::Types::String)
                    return false;
                if (res)
                    *res = field.safeGet<String>();
                return true;
            }

            if (allow_identifier && isIdentifier())
            {
                if (res)
                    *res = argument->result_name;
                return true;
            }

            return false;
        }
        bool tryGetLiteralText(String * res) const override
        {
            Field field;
            if (!tryGetConstantField(field))
                return false;
            if (res)
                *res = applyVisitor(FieldVisitorToString(), field);
            return true;
        }
    private:
        static const ActionsDAG::Node * unwrapAlias(const ActionsDAG::Node * node)
        {
            while (node->type == ActionsDAG::ActionType::ALIAS)
                node = node->children.front();
            return node;
        }
        bool tryGetConstantField(Field & field) const
        {
            if (!argument->column || !isColumnConst(*argument->column))
                return false;
            field = assert_cast<const ColumnConst &>(*argument->column).getField();
            return true;
        }
        const ActionsDAG::Node * argument = nullptr;
    };

    class ArgumentsActionsDAG : public Arguments
    {
    public:
        explicit ArgumentsActionsDAG(const ActionsDAG::NodeRawConstPtrs * arguments_) : arguments(arguments_) {}
        size_t size() const override { return arguments ? arguments->size() : 0; }
        std::unique_ptr<Argument> at(size_t n) const override
        {
            return std::make_unique<ArgumentActionsDAG>(arguments->at(n));
        }
    private:
        const ActionsDAG::NodeRawConstPtrs * arguments = nullptr;
    };

    explicit FunctionActionsDAG(const ActionsDAG::Node & function_) : function(&function_)
    {
        if (!function->children.empty())
            arguments = std::make_unique<ArgumentsActionsDAG>(&function->children);
    }

    String name() const override { return function->function_base->getName(); }
private:
    const ActionsDAG::Node * function = nullptr;
};

/// Finds secret arguments of a FUNCTION node of an `ActionsDAG`. A DAG function is always an ordinary
/// function (table engines, database engines and backup names never become DAG nodes).
class FunctionSecretArgumentsFinderActionsDAG : public FunctionSecretArgumentsFinder
{
public:
    explicit FunctionSecretArgumentsFinderActionsDAG(const ActionsDAG::Node & function_)
        : FunctionSecretArgumentsFinder(std::make_unique<FunctionActionsDAG>(function_))
    {
        if (!function->hasArguments())
            return;

        findOrdinaryFunctionSecretArguments();
    }
};

}
