#pragma once

#include <map>
#include <memory>
#include <string>
#include <cstring>
#include <vector>
#include <variant>

#include <ext/singleton.h>

#include <Core/Field.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Parsers/IParser.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace FunctionSignature
{

/** Function signature is responsible for:
  * - checking types (and const-ness) of function arguments;
  * - getting result type by types of arguments and values of constant arguments;
  * - printing documentation about function signature;
  * - printing well understandable error messages in case of types/constness mismatch.
  *
  * For the purpuse of the latter (introspectiveness), the function signature must be declarative
  *  and constructed from simple parts.
  *
  *
  * It's not trivial. Let's consider the following examples:
  *
  * transform(T, Array(T), Array(U), U) -> U
  * - here we have dependency between types of arguments;
  *
  * array(T1...) -> Array(LeastCommonType(T1...))
  * - here we have variable number of arguments and there must exist least common type for all arguments.
  *
  * toFixedString(String, N) -> FixedString(N)
  * tupleElement(T : Tuple, N) -> TypeOfTupleElement(T, N)
  * - here N must be constant expression and the result type is dependent of its value.
  *
  * arrayReduce(agg String, ...) -> return type of aggregate function 'agg'
  *
  * multiIf(cond1 UInt8, then1 T1, [cond2 UInt8, then2 T2, ...], else U) -> LeastCommonType(T1, T2, ..., U)
  * - complicated recursive signature.
  */

/** Function signature consists of:
  *
  * func_name(args_description) -> return_desription
  * where constraints
  * or alternative_signatures
  *
  * func_name - function name for documentation purposes.
  * args_description - zero or more comma separated descriptions of arguments.
  *
  * argument_description:
  *
  * const arg_name type_description
  *
  * const - optional specification that the argument must be constant
  * arg_name - optional name of argument
  *
  * type_description:
  *
  * TypeName
  * TypeMatcher
  * TypeName : TypeMatcher
  *
  * TypeName - name of a type like String or type variable like T
  * TypeMatcher - name of type matcher like UnsignedInteger or function-like expression of other type desciptions,
  *  like Array(T : UsignedInteger) or constant descriptions like FixedString(N)
  *
  * or argument_description may be ellipsis:
  * ...
  * or an ellipsis with number capture:
  * ...(N)
  *
  * Ellipsis means:
  * - if it is the only argument description: any number of any arguments;
  * - repeat previous argument or a group of previous arguments with the same numeric suffix of names, zero or more times;
  * - the number of reprtitions is captured or compared to variable N.
  */


/** Every variable can act as an array. It is intended for implementation of ellipsis (...) - recursive function signatures.
  * Values of variables are keyed by variable name and index.
  * Zero index is implicit if index is not specified.
  */
struct Key
{
    std::string name;
    size_t index = 0;

    auto tuple() const { return std::tie(name, index); }
    bool operator==(const Key & rhs) const { return tuple() == rhs.tuple(); }
    bool operator<(const Key & rhs) const { return tuple() < rhs.tuple(); }

    std::string toString() const { return index ? name + DB::toString(index) : name; }

    Key() {}

    Key(const std::string & str) : name(str)
    {
        if (name.empty())
            throw Exception("Logical error: empty variable name", ErrorCodes::LOGICAL_ERROR);

        const char * begin = name.data();
        const char * end = begin + name.size();
        const char * pos = end - 1;

        while (pos > begin && isNumericASCII(*pos))
            --pos;

        if (!isNumericASCII(*pos))
            ++pos;

        if (pos < end)
            index = parse<size_t>(pos);

        name.resize(pos - name.data());
    }

    /// Increment index if it exists.
    Key incrementIndex(size_t amount) const
    {
        Key res = *this;
        if (res.index)
            res.index += amount;
        return res;
    }
};


inline size_t getCommonIndex(size_t i, size_t j)
{
    if (i && j && i != j)
        throw Exception("Different indices of variables in subexpression", ErrorCodes::LOGICAL_ERROR);
    return i ? i : j;
}

/** Captured type or a value of a constant.
  */
struct Value
{
    std::variant<DataTypePtr, Field> value;

    Value(const DataTypePtr & type) : value(type) {}
    Value(const Field & field) : value(field) {}

    bool operator==(const Value & rhs) const
    {
        return value.index() == rhs.value.index()
            && ((value.index() == 0 && std::get<DataTypePtr>(value)->equals(*std::get<DataTypePtr>(rhs.value)))
                || (value.index() == 1 && std::get<Field>(value) == std::get<Field>(rhs.value)));
    }

    const DataTypePtr & type() const
    {
        return std::get<DataTypePtr>(value);
    }

    /// For implementation of constraints.
    bool isTrue() const
    {
        return std::get<Field>(value).safeGet<UInt64>() == 1;
    }
};


/** Scratch space for checking function signature.
  * Variables are either assigned or checked for equality, but never reassigned.
  */
struct Variables
{
    using Container = std::map<Key, Value>;
    Container container;

    bool has(const Key & key) const
    {
        return container.count(key);
    }

    bool assignOrCheck(const Key & key, const Value & var)
    {
        if (auto it = container.find(key); it == container.end())
        {
            container.emplace(key, var);
            return true;
        }
        else
            return it->second == var;
    }

    bool assignOrCheck(const Key & key, const DataTypePtr & type)
    {
        return assignOrCheck(key, Value(type));
    }

    bool assignOrCheck(const Key & key, const Field & const_value)
    {
        return assignOrCheck(key, Value(const_value));
    }

    Value get(const Key & key) const
    {
        if (auto it = container.find(key); it != container.end())
            return it->second;
        else
            throw Exception("Variable " + key.toString() + " was not captured", ErrorCodes::LOGICAL_ERROR);
    }
};


/** A list of arguments for type function or constraint.
  */
using Values = std::vector<Value>;


/** List of variables to assign or check.
  * Empty string means that argument is unused.
  * Instead of a variable name, ellipsis ("...") may be given. It will be a subject for expansion.
  */
using Keys = std::vector<Key>;


template <typename What, typename... Args>
class Factory : public ext::singleton<Factory<What, Args...>>
{
private:
    using Creator = std::function<What(Args...)>;
    using Dict = std::map<std::string, Creator>;

    Dict dict;
public:
    void registerElement(const std::string & name, const Creator & creator)
    {
        if (!dict.emplace(name, creator).second)
            throw Exception("Element " + name + " is already registered in factory", ErrorCodes::LOGICAL_ERROR);
    }

    What tryGet(const std::string & name, Args &&... args) const
    {
        if (auto it = dict.find(name); it != dict.end())
            return it->second(std::forward<Args>(args)...);
        return {};
    }

    What get(const std::string & name, Args &&... args) const
    {
        if (auto res = tryGet(name, std::forward<Args>(args)...))
            return res;
        else
            throw Exception("Unknown element: " + name, ErrorCodes::LOGICAL_ERROR);
    }
};


/** Check type to match some criteria, possibly set some variables.
  * iteration - all variable indices are incremented by this number - used for implementation of ellipsis.
  */
class ITypeMatcher
{
public:
    virtual ~ITypeMatcher() {}
    virtual std::string name() const = 0;
    virtual bool match(const DataTypePtr & what, Variables & vars, size_t iteration) const = 0;

    /// Extract common index of variables participating in expression.
    virtual size_t getIndex() const = 0;
};

using TypeMatcherPtr = std::shared_ptr<ITypeMatcher>;
using TypeMatchers = std::vector<TypeMatcherPtr>;
using TypeMatcherFactory = Factory<TypeMatcherPtr, const TypeMatchers &>;

/// Matches any type.
class AnyTypeMatcher : public ITypeMatcher
{
public:
    std::string name() const override { return "Any"; }
    bool match(const DataTypePtr &, Variables &, size_t) const override { return true; }
    size_t getIndex() const override { return 0; }
};


/// Matches exact type.
class ExactTypeMatcher : public ITypeMatcher
{
private:
    DataTypePtr type;

public:
    ExactTypeMatcher(const DataTypePtr & type) : type(type) {}

    std::string name() const override
    {
        return type->getName();
    }

    bool match(const DataTypePtr & what, Variables &, size_t) const override
    {
        return type->equals(*what);
    }

    size_t getIndex() const override { return 0; }
};


/// Use another matcher and assign or check the matched type to the variable.
class AssignTypeMatcher : public ITypeMatcher
{
private:
    TypeMatcherPtr impl;
    Key var_key;

public:
    AssignTypeMatcher(const TypeMatcherPtr & impl, const Key & var_key) : impl(impl), var_key(var_key) {}

    std::string name() const override
    {
        return impl->name();
    }

    bool match(const DataTypePtr & what, Variables & vars, size_t iteration) const override
    {
        if (!impl->match(what, vars, iteration))
            return false;

        return vars.assignOrCheck(var_key.incrementIndex(iteration), what);
    }

    size_t getIndex() const override
    {
        return getCommonIndex(var_key.index, impl->getIndex());
    }
};


struct ArgumentDescription
{
    bool is_const = false;
    Key argument_name;
    TypeMatcherPtr type_matcher;

    bool match(const DataTypePtr & type, const ColumnPtr & column, Variables & vars, size_t iteration) const
    {
        if (is_const && (!column || !column->isColumnConst()))
            return false;

        if (is_const && !vars.assignOrCheck(argument_name.incrementIndex(iteration), typeid_cast<const ColumnConst &>(*column).getField()))
            return false;

        std::cerr << type_matcher->name() << "\n";
        return type_matcher->match(type, vars, iteration);
    }

    /// Extract common index of variables participating in expression.
    size_t getIndex() const
    {
        return getCommonIndex(argument_name.index, type_matcher->getIndex());
    }
};

using ArgumentsDescription = std::vector<ArgumentDescription>;

struct ArgumentsGroup
{
    ArgumentsDescription elems;
    enum Type
    {
        NoType,
        Fixed,      /// As is
        Optional,   /// Zero or one time
        Ellipsis    /// Zero or any number of times
    } type = NoType;

    bool match(const ColumnsWithTypeAndName & args, Variables & vars, size_t offset, size_t iteration) const
    {
        std::cerr << "^\n";
        size_t size = elems.size();

        if (size + offset > args.size())
            return false;

        std::cerr << "^^\n";

        for (size_t i = 0; i < size; ++i)
        {
            const auto & col = args[offset + i];
            if (!elems[i].match(col.type, col.column, vars, iteration))
                return false;

            std::cerr << "^^^\n";
        }
        return true;
    }
};

using ArgumentsGroups = std::vector<ArgumentsGroup>;

struct VariadicArguments
{
    ArgumentsGroups groups;

    bool match(const ColumnsWithTypeAndName & args, Variables & vars, size_t group_offset, size_t args_offset, size_t iteration) const
    {
        if (group_offset == groups.size() && args_offset == args.size())
            return true;
        if (group_offset >= groups.size() || args_offset >= args.size())
            return false;

        std::cerr << "group_offset: " << group_offset << "\n";
        std::cerr << "groups.size(): " << groups.size() << "\n";

        const ArgumentsGroup & group = groups[group_offset];
        switch (group.type)
        {
            case ArgumentsGroup::Fixed:
                return group.match(args, vars, args_offset, 0)
                    && match(args, vars, group_offset + 1, args_offset + group.elems.size(), iteration);

            case ArgumentsGroup::Optional:
                return match(args, vars, group_offset + 1, args_offset + group.elems.size(), iteration) /// Skip group
                    || (group.match(args, vars, args_offset, 0)                                         /// Match group
                        && match(args, vars, group_offset + 1, args_offset + group.elems.size(), iteration)); /// and continue from next group

            case ArgumentsGroup::Ellipsis:
                return match(args, vars, group_offset + 1, args_offset + group.elems.size(), iteration) /// Skip group
                    || (group.match(args, vars, args_offset, iteration + 1)                                   /// Match group
                        && match(args, vars, group_offset, args_offset + group.elems.size(), iteration + 1)); ///  and try to match again
            default:
                throw Exception("Wrong type of ArgumentsGroup", ErrorCodes::LOGICAL_ERROR);
        }
    }
};


/** A function of variables (types and constants) that returns variable (type or constant).
  * It also may return nullptr type that means that it is not applicable.
  */
class ITypeFunction
{
public:
    virtual ~ITypeFunction() {}
    virtual Value apply(const Values & args) const = 0;
};

using TypeFunctionPtr = std::shared_ptr<ITypeFunction>;
using TypeFunctionFactory = Factory<TypeFunctionPtr>;


/** Part of expression tree that contains type functions, variables and constants.
  */
class ITypeExpression
{
public:
    virtual ~ITypeExpression() {}
    virtual Value apply(const Variables & context, size_t iteration) const = 0;

    /// All variables from context that the function depends on are available.
    virtual bool hasEnoughContext(const Variables & context, size_t iteration) const = 0;

    /// Extract common index of variables participating in expression.
    virtual size_t getIndex() const = 0;
};

using TypeExpressionPtr = std::shared_ptr<ITypeExpression>;
using TypeExpressions = std::vector<TypeExpressionPtr>;

/// Takes no arguments and returns a value of variable from context.
class VariableTypeExpression : public ITypeExpression
{
private:
    Key key;
public:
    VariableTypeExpression(const Key & key) : key(key) {}

    Value apply(const Variables & context, size_t iteration) const override
    {
        return context.get(key.incrementIndex(iteration));
    }

    bool hasEnoughContext(const Variables & context, size_t iteration) const override
    {
        std::cerr << key.incrementIndex(iteration).toString() << ", " << (int)context.has(key.incrementIndex(iteration)) << "\n";
        return context.has(key.incrementIndex(iteration));
    }

    size_t getIndex() const override
    {
        return key.index;
    }
};

/// Takes zero arguments and returns pre-determined value. It allows to represent a value as a function without arguments.
class ConstantTypeExpression : public ITypeExpression
{
private:
    Value res;
public:
    ConstantTypeExpression(const Value & res) : res(res) {}

    Value apply(const Variables &, size_t) const override
    {
        return res;
    }

    bool hasEnoughContext(const Variables &, size_t) const override
    {
        return true;
    }

    size_t getIndex() const override
    {
        return 0;
    }
};

class TypeExpressionTree : public ITypeExpression
{
private:
    TypeFunctionPtr func;
    TypeExpressions children;   /// nullptr child means ellipsis

public:
    TypeExpressionTree(const TypeFunctionPtr & func, const TypeExpressions & children) : func(func), children(children) {}

    Value apply(const Variables & context, size_t iteration) const override
    {
        Values args;

        /// Accumulate a group of children to repeat when ellipsis is encountered.
        TypeExpressions group_to_repeat;
        size_t prev_index = 0;

        for (const auto & child : children)
        {
            if (child)
            {
                args.emplace_back(child->apply(context, iteration));

                size_t current_index = child->getIndex();
                if (!current_index || (prev_index && prev_index != current_index))
                    group_to_repeat.clear();
                else if (current_index)
                    group_to_repeat.emplace_back(child);
            }
            else
            {
                if (group_to_repeat.empty())
                     throw Exception("No group to repeat in type function", ErrorCodes::LOGICAL_ERROR);

                std::cerr << "group size: " << group_to_repeat.size() << "\n";

                /// Repeat accumulated group of children while there are enough variables in context.
                size_t repeat_iteration = iteration;
                while (true)
                {
                    ++repeat_iteration;

                    std::cerr << "repeat iteration: " << repeat_iteration << "\n";

                    auto it = group_to_repeat.begin();
                    for (; it != group_to_repeat.end(); ++it)
                        if (!(*it)->hasEnoughContext(context, repeat_iteration))
                            break;
                    if (it != group_to_repeat.end())
                        break;

                    for (it = group_to_repeat.begin(); it != group_to_repeat.end(); ++it)
                        args.emplace_back((*it)->apply(context, repeat_iteration));
                }
            }
        }
        return func->apply(args);
    }

    bool hasEnoughContext(const Variables & context, size_t iteration) const override
    {
        for (const auto & child : children)
            if (!child->hasEnoughContext(context, iteration))
                return false;
        return true;
    }

    size_t getIndex() const override
    {
        size_t res = 0;
        for (const auto & child : children)
            res = getCommonIndex(res, child->getIndex());
        return res;
    }
};


class IFunctionSignature
{
public:
    virtual ~IFunctionSignature() {}
    virtual DataTypePtr check(const ColumnsWithTypeAndName & args) const = 0;
};

using FunctionSignaturePtr = std::shared_ptr<IFunctionSignature>;
using FunctionSignatures = std::vector<FunctionSignaturePtr>;


struct VariadicFunctionSignature : public IFunctionSignature
{
    VariadicArguments arguments_description;
    TypeExpressionPtr return_type;
    TypeExpressions constraints;

    DataTypePtr check(const ColumnsWithTypeAndName & args) const override
    {
        /// Apply type matchers and assign variables.

        Variables vars;

        std::cerr << "%\n";

        if (!arguments_description.match(args, vars, 0, 0, 0))
            return nullptr;

        std::cerr << "%%\n";

        /// Check constraints against variables.

        for (const TypeExpressionPtr & constraint : constraints)
            if (!constraint->apply(vars, 0).isTrue())
                return nullptr;

        std::cerr << "%%%\n";

        return std::get<DataTypePtr>(return_type->apply(vars, 0).value);
    }
};


struct AlternativeFunctionSignature : public IFunctionSignature
{
    FunctionSignatures alternatives;

    DataTypePtr check(const ColumnsWithTypeAndName & args) const override
    {
        for (const auto & alternative : alternatives)
            if (DataTypePtr res = alternative->check(args))
                return res;
        return nullptr;
    }
};


/** Grammar (kinda):
  *
  * simple_signature ::= identifier '(' arguments_list ')' '->' type_func
  * simple_signature_with_constraints ::= simple_signature ('WHERE' constraints_list)?
  * alternative_signature ::= simple_signature_with_constraints ('OR' simple_signature_with_constraints)?

  * constraints_list ::= identifier '(' variables_list ')'
  * type_func ::= identifier | identifier '(' variables_list ')'
  * variables_list ::= (identifier (',' identifier_or_ellipsis)?)?
  * arguments_list ::= (argument_description ',' ...)?
  *
  * argument_description ::= ellipsis | "const"? identifier? type_matcher
  * type_matcher ::= identifier | type_matcher_func | identifier ':' type_matcher_func
  * type_matcher_func ::= identifier '(' type_matcher_list ')'
  * type_matcher_list ::= (type_matcher ',' ...)?
  */

bool parseIdentifier(TokenIterator & pos, std::string & res)
{
    if (pos->type == TokenType::BareWord)
    {
        res.assign(pos->begin, pos->end);
        std::cerr << "Parsed identifier " << res << "\n";
        ++pos;
        return true;
    }
    return false;
}

bool consumeToken(TokenIterator & pos, TokenType type)
{
    if (pos->type == type)
    {
        std::cerr << "Consuming " << std::string(pos->begin, pos->end) << "\n";
        ++pos;
        return true;
    }
    return false;
}

bool consumeKeyword(TokenIterator & pos, const std::string & keyword)
{
    if (pos->type == TokenType::BareWord)
    {
        if (pos->size() != keyword.size() || strncasecmp(pos->begin, keyword.data(), pos->size()))
            return false;
        ++pos;
        return true;
    }
    return false;
}

template <typename ParseElem, typename ParseDelimiter>
bool parseList(TokenIterator & pos, bool allow_empty, ParseElem && parse_elem, ParseDelimiter && parse_delimiter)
{
    if (!parse_elem(pos))
        return allow_empty;

    auto prev_pos = pos;
    while (parse_delimiter(pos))
    {
        if (!parse_elem(pos))
        {
            /// step back before delimiter. This is important for parsing lists of lists like "a, [b, c], d".
            pos = prev_pos;
            return true;
        }
        prev_pos = pos;
    }
    return true;
}

template <typename ParseArgument>
bool parseFunctionLikeExpression(TokenIterator & pos, std::string & name, bool allow_no_arguments, ParseArgument && parse_argument)
{
    if (!parseIdentifier(pos, name))
        return false;

    if (!consumeToken(pos, TokenType::OpeningRoundBracket))
        return allow_no_arguments;

    return parseList(pos, true,
        parse_argument,
        [](TokenIterator & pos)
        {
            return consumeToken(pos, TokenType::Comma);
        })
        && consumeToken(pos, TokenType::ClosingRoundBracket);
}

bool parseTypeExpression(TokenIterator & pos, TypeExpressionPtr & res)
{
    TokenIterator begin = pos;
    std::string name;
    TypeExpressions children;
    if (parseFunctionLikeExpression(pos, name, true,
        [&](TokenIterator & pos)
        {
            TypeExpressionPtr elem;
            if (parseTypeExpression(pos, elem) || consumeToken(pos, TokenType::Ellipsis))
            {
                children.emplace_back(elem);
                return true;
            }
            return false;
        }))
    {
        /// Examples:
        /// - variable:      f(T) -> T
        /// - type:          f() -> UInt8
        /// - type function: f(A, B) -> LeastCommonType(A, B)

        TypeFunctionPtr func = TypeFunctionFactory::instance().tryGet(name);
        if (func)
        {
            std::cerr << "Type func\n";
            res = std::make_shared<TypeExpressionTree>(func, children);
            return true;
        }

        /// Exact type.

        const auto & factory = DataTypeFactory::instance();
        if (factory.existsCanonicalFamilyName(name))
        {
            std::cerr << "Exact type\n";
            auto prev_pos = pos;
            --prev_pos;
            const std::string full_name(begin->begin, prev_pos->end);
            res = std::make_shared<ConstantTypeExpression>(factory.get(full_name));
            return true;
        }

        /// Type variable (example: T)

        if (children.empty())
        {
            std::cerr << "Type variable\n";
            res = std::make_shared<VariableTypeExpression>(name);
            return true;
        }

        throw Exception("Unknown type function: " + name, ErrorCodes::LOGICAL_ERROR);
    }
    return false;
}

bool parseTypeMatcher(TokenIterator & pos, TypeMatcherPtr & res);

bool parseSimpleTypeMatcher(TokenIterator & pos, TypeMatcherPtr & res)
{
    TokenIterator begin = pos;
    std::string name;
    TypeMatchers args;

    if (parseFunctionLikeExpression(pos, name, true,
        [&](TokenIterator & pos)
        {
            TypeMatcherPtr elem;
            if (parseTypeMatcher(pos, elem))
            {
                args.emplace_back(elem);
                return true;
            }
            return false;
        }))
    {
        res = TypeMatcherFactory::instance().tryGet(name, args);
        if (res)
            return true;

        /// Exact type (example: UInt8).

        const auto & factory = DataTypeFactory::instance();
        const std::string family_name(begin->begin, begin->end);
        if (factory.existsCanonicalFamilyName(family_name))
        {
            auto prev_pos = pos;
            --prev_pos;
            const std::string full_name(begin->begin, prev_pos->end);
            res = std::make_shared<ExactTypeMatcher>(factory.get(full_name));
            return true;
        }

        /// Type variable (example: T)

        if (args.empty())
        {
            res = std::make_shared<AssignTypeMatcher>(std::make_shared<AnyTypeMatcher>(), name);
            return true;
        }

        throw Exception("Unknown type matcher: " + name, ErrorCodes::LOGICAL_ERROR);
    }
    return false;
}

bool parseTypeMatcher(TokenIterator & pos, TypeMatcherPtr & res)
{
    /// Matcher
    /// Matcher(...)
    /// T : Matcher
    /// T : Matcher(...)

    auto next_pos = pos;
    ++next_pos;

    if (consumeToken(next_pos, TokenType::Colon))
    {
        std::cerr << "$\n";

        std::string var_name;
        if (!parseIdentifier(pos, var_name))
            return false;

        std::cerr << "$$\n";

        if (!parseSimpleTypeMatcher(next_pos, res))
            return false;

        std::cerr << "$$$\n";

        pos = next_pos;
        res = std::make_shared<AssignTypeMatcher>(res, var_name);
        return true;
    }
    else
        return parseSimpleTypeMatcher(pos, res);
}

bool parseSimpleArgumentDescription(TokenIterator & pos, ArgumentDescription & res)
{
    if (consumeKeyword(pos, "const"))
        res.is_const = true;

    auto next_pos = pos;
    ++next_pos;

    /// arg_name T : Matcher
    /// arg_name T : Matcher(...)
    /// arg_name Matcher
    /// T : Matcher
    /// T : Matcher(...)
    /// Matcher

    /// If arg_name present, consume it.
    if (pos->type == TokenType::BareWord && next_pos->type == TokenType::BareWord)
    {
        std::string identifier;
        if (!parseIdentifier(pos, identifier))
            return false;
        res.argument_name = identifier;
    }

    return parseTypeMatcher(pos, res.type_matcher);
}

bool parseSimpleArgumentsDescription(TokenIterator & pos, ArgumentsDescription & res)
{
    return parseList(pos, false,
        [&](TokenIterator & pos)
        {
            ArgumentDescription arg;
            if (!parseSimpleArgumentDescription(pos, arg))
                return false;
            res.emplace_back(arg);
            std::cerr << "%\n";
            return true;
        },
        [](TokenIterator & pos)
        {
            return consumeToken(pos, TokenType::Comma);
        });
}

bool parseArgumentsGroup(TokenIterator & pos, ArgumentsGroup & res, const ArgumentsGroup & prev_group)
{
    if (consumeToken(pos, TokenType::OpeningSquareBracket)
        && parseSimpleArgumentsDescription(pos, res.elems)
        && consumeToken(pos, TokenType::ClosingSquareBracket))
    {
        res.type = ArgumentsGroup::Optional;
        return true;
    }

    if (parseSimpleArgumentsDescription(pos, res.elems))
    {
        res.type = ArgumentsGroup::Fixed;
        return true;
    }

    if (consumeToken(pos, TokenType::Ellipsis))
    {
        res.type = ArgumentsGroup::Ellipsis;

        if (prev_group.type == ArgumentsGroup::NoType)
        {
            ArgumentDescription arg;
            arg.type_matcher = std::make_shared<AnyTypeMatcher>();
            res.elems.emplace_back(std::move(arg));
            return true;
        }
        else if (prev_group.type == ArgumentsGroup::Fixed)
        {
            size_t prev_index = 0;
            for (auto it = prev_group.elems.rbegin(); it != prev_group.elems.rend(); ++it)
            {
                size_t current_index = it->getIndex();
                if (!current_index)
                {
                    res.elems.emplace_back(*it);
                    break;
                }

                if (!prev_index)
                    prev_index = current_index;
                else if (prev_index != current_index)
                    break;

                res.elems.emplace(res.elems.begin(), *it);
            }
            return true;
        }
        else if (prev_group.type == ArgumentsGroup::Optional)
        {
            /// Repeat the whole group.
            res.elems = prev_group.elems;
            return true;
        }
        else
            throw Exception("Bad arguments group before ellipsis", ErrorCodes::LOGICAL_ERROR);
    }

    return false;
}

bool parseVariadicFunctionSignature(TokenIterator & pos, VariadicFunctionSignature & res)
{
    std::cerr << "@\n";

    std::string name;
    if (parseFunctionLikeExpression(pos, name, true,
        [&](TokenIterator & pos)
        {
            std::cerr << "@@\n";
            return parseList(pos, true,
                [&](TokenIterator & pos)
                {
                    std::cerr << "@@@\n";
                    ArgumentsGroup group;
                    if (parseArgumentsGroup(pos, group, res.arguments_description.groups.empty() ? ArgumentsGroup() : res.arguments_description.groups.back()))
                    {
                        std::cerr << "@@@@\n";
                        res.arguments_description.groups.emplace_back(group);
                        return true;
                    }
                    return false;
                },
                [](TokenIterator & pos)
                {
                    return consumeToken(pos, TokenType::Comma);
                });
        })
        && consumeToken(pos, TokenType::Arrow)
        && parseTypeExpression(pos, res.return_type))
    {
        std::cerr << "#\n";
        if (consumeKeyword(pos, "WHERE"))
        {
            if (!parseList(pos, false,
                [&](TokenIterator & pos)
                {
                    TypeExpressionPtr constraint;
                    if (parseTypeExpression(pos, constraint))
                    {
                        res.constraints.emplace_back(constraint);
                        return true;
                    }
                    return false;
                },
                [](TokenIterator & pos)
                {
                    return consumeToken(pos, TokenType::Comma);
                }))
            {
                return false;
            }
        }

        return true;
    }

    return false;
}

bool parseAlternativeFunctionSignature(TokenIterator & pos, FunctionSignaturePtr & res)
{
    auto signature = std::make_shared<AlternativeFunctionSignature>();
    std::cerr << "!\n";
    if (parseList(pos, false,
        [&](TokenIterator & pos)
        {
            std::cerr << "?\n";
            auto alternative = std::make_shared<VariadicFunctionSignature>();
            if (parseVariadicFunctionSignature(pos, *alternative))
            {
                signature->alternatives.emplace_back(alternative);
                return true;
            }
            return false;
        },
        [](TokenIterator & pos)
        {
            std::cerr << "??\n";
            return consumeKeyword(pos, "OR");
        }))
    {
        std::cerr << "!!\n";
        res = std::move(signature);
        return true;
    }

    return false;
}

bool parseFunctionSignature(TokenIterator & pos, FunctionSignaturePtr & res)
{
    return parseAlternativeFunctionSignature(pos, res);
}

}

}
