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
struct VariableKey
{
    std::string name;
    size_t index = 0;

    auto tuple() const { return std::tie(name, index); }
    bool operator==(const VariableKey & rhs) const { return tuple() == rhs.tuple(); }
    bool operator<(const VariableKey & rhs) const { return tuple() < rhs.tuple(); }

    std::string toString() const { return index ? name + DB::toString(index) : name; }

    VariableKey() {}

    VariableKey(const std::string & str) : name(str)
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

    VariableKey changeIndex(size_t new_index) const
    {
        VariableKey res = *this;
        res.index = new_index;
        return res;
    }

    VariableKey incrementIndex(size_t amount) const
    {
        return changeIndex(index + amount);
    }
};

/** Captured type or a value of a constant.
  */
struct VariableValue
{
    std::variant<DataTypePtr, Field> value;

    VariableValue(const DataTypePtr & type) : value(type) {}
    VariableValue(const Field & field) : value(field) {}

    bool operator==(const VariableValue & rhs) const
    {
        return value.index() == rhs.value.index()
            && ((value.index() == 0 && std::get<DataTypePtr>(value)->equals(*std::get<DataTypePtr>(rhs.value)))
                || (value.index() == 1 && std::get<Field>(value) == std::get<Field>(rhs.value)));
    }
};


/** Scratch space for checking function signature.
  * Variables are either assigned or checked for equality, but never reassigned.
  */
struct Variables
{
    using Container = std::map<VariableKey, VariableValue>;
    Container container;

    bool has(const VariableKey & key) const
    {
        return container.count(key);
    }

    bool assignOrCheck(const VariableKey & key, const VariableValue & var)
    {
        if (auto it = container.find(key); it == container.end())
        {
            container.emplace(key, var);
            return true;
        }
        else
            return it->second == var;
    }

    bool assignOrCheck(const VariableKey & key, const DataTypePtr & type)
    {
        return assignOrCheck(key, VariableValue(type));
    }

    bool assignOrCheck(const VariableKey & key, const Field & const_value)
    {
        return assignOrCheck(key, VariableValue(const_value));
    }
};


/** A list of arguments for type function or constraint.
  */
using VariablesList = std::vector<VariableValue>;


/** List of variables to assign or check.
  * Empty string means that argument is unused.
  * Instead of a variable name, ellipsis ("...") may be given. It will be a subject for expansion.
  */
using VariableKeys = std::vector<VariableKey>;


inline VariableKeys expandEllipsis(const Variables & vars, const VariableKeys & keys)
{
    VariableKeys new_keys;

    size_t length_of_group = 0;
    size_t prev_variable_index = 0;

    for (size_t i = 0, size = keys.size(); i < size; ++i)
    {
        const VariableKey & key = keys[i];

        if (!vars.has(key))
            throw Exception("Logical error: no variable named " + key.toString() + " exists in function signature", ErrorCodes::LOGICAL_ERROR);

        if (key.name == "...")
        {
            if (!prev_variable_index || !length_of_group)
                throw Exception("Logical error: bad ellipsis, zero length of group to repeat", ErrorCodes::LOGICAL_ERROR);

            size_t current_variable_index = prev_variable_index + 1;

            while (true)
            {
                size_t idx_in_group = 0;
                for (; idx_in_group < length_of_group; ++idx_in_group)
                    if (!vars.has(keys[i - length_of_group + idx_in_group].changeIndex(current_variable_index)))
                        break;

                if (idx_in_group == length_of_group)
                {
                    for (size_t idx_in_group_2 = 0; idx_in_group_2 < length_of_group; ++idx_in_group_2)
                        new_keys.emplace_back(keys[i - length_of_group + idx_in_group_2].changeIndex(current_variable_index));
                }
                else
                    break;

                ++current_variable_index;
            }

            prev_variable_index = 0;
            length_of_group = 0;
        }
        else
        {
            if (key.index)
            {
                if (prev_variable_index == key.index)
                {
                    ++length_of_group;
                }
                else
                {
                    prev_variable_index = key.index;
                    length_of_group = 1;
                }
            }
            else
            {
                prev_variable_index = 0;
                length_of_group = 0;
            }

            new_keys.emplace_back(key);
        }
    }

    return new_keys;
}


/** Check type to match some criteria, possibly set some variables.
  * iteration - all variable indices are incremented by this number - used for implementation of ellipsis.
  */
class ITypeMatcher
{
public:
    virtual ~ITypeMatcher() {}
    virtual std::string name() const = 0;
    virtual bool match(const DataTypePtr & what, Variables & vars, size_t iteration) const = 0;
};

using TypeMatcherPtr = std::shared_ptr<ITypeMatcher>;
using TypeMatchers = std::vector<TypeMatcherPtr>;


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

/** Find type matcher by its name.
  */
using TypeMatcherFactory = Factory<TypeMatcherPtr, const TypeMatchers &>;


/// Matches any type.
class AnyTypeMatcher : public ITypeMatcher
{
public:
    std::string name() const override { return "Any"; }
    bool match(const DataTypePtr &, Variables &, size_t) const override { return true; }
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
};


/// Use another matcher and assign or check the matched type to the variable.
class AssignTypeMatcher : public ITypeMatcher
{
private:
    TypeMatcherPtr impl;
    VariableKey var_key;

public:
    AssignTypeMatcher(const TypeMatcherPtr & impl, const VariableKey & var_key) : impl(impl), var_key(var_key) {}

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
};


struct ArgumentDescription
{
    bool is_const = false;
    VariableKey argument_name;
    TypeMatcherPtr type_matcher;
    size_t iteration = 0;   /// Repeating arguments after expansion of ellipsis.

    bool match(const DataTypePtr & type, const ColumnPtr & column, Variables & vars) const
    {
        if (is_const && (!column || !column->isColumnConst()))
            return false;

        if (is_const && !vars.assignOrCheck(argument_name.incrementIndex(iteration), typeid_cast<const ColumnConst &>(*column).getField()))
            return false;

        return type_matcher->match(type, vars, iteration);
    }
};

using ArgumentsDescription = std::vector<ArgumentDescription>;


/** A function of variables (types and constants) that returns a type.
  * It also may return nullptr that means that it is not applicable.
  */
class ITypeFunction
{
public:
    virtual ~ITypeFunction() {}
    virtual DataTypePtr apply(const VariablesList & vars) const = 0;
};

using TypeFunctionPtr = std::shared_ptr<ITypeFunction>;
using TypeFunctionFactory = Factory<TypeFunctionPtr>;

/// Takes one type-variable and returns it.
class IdentityTypeFunction : public ITypeFunction
{
public:
    DataTypePtr apply(const VariablesList & vars) const override
    {
        if (vars.size() != 1)
            throw Exception("Logical error: identity type function have not a single argument", ErrorCodes::LOGICAL_ERROR);

        auto res = std::get<DataTypePtr>(vars[0].value);
        if (!res)
            throw Exception("Logical error: not a type argument is passed to identity type function", ErrorCodes::LOGICAL_ERROR);

        return res;
    }
};

/// Takes zero arguments and returns pre-determined type.
class FixedTypeFunction : public ITypeFunction
{
private:
    DataTypePtr res;
public:
    FixedTypeFunction(const DataTypePtr & res) : res(res) {}

    DataTypePtr apply(const VariablesList & vars) const override
    {
        if (vars.size() != 0)
            throw Exception("Logical error: non-empty arguments list passed to fixed type function", ErrorCodes::LOGICAL_ERROR);
        return res;
    }
};

struct BoundTypeFunction
{
    TypeFunctionPtr func;
    VariableKeys args; /// TODO Allow nested expressions with type functions.

    DataTypePtr apply(const Variables & vars) const
    {
        VariablesList func_vars;
        func_vars.reserve(args.size());
        for (const auto & key : args)
            func_vars.emplace_back(vars.container.at(key));

        return func->apply(func_vars);
    }
};


/** Check some criteria on variables (types and constants).
  */
class IConstraint
{
public:
    virtual ~IConstraint() {}
    virtual bool check(const VariablesList & vars) const = 0;
};

using ConstraintPtr = std::shared_ptr<IConstraint>;
using ConstraintFactory = Factory<ConstraintPtr>;

struct BoundConstraint
{
    ConstraintPtr constraint;
    VariableKeys args;

    bool check(const Variables & vars) const
    {
        VariablesList func_vars;
        func_vars.reserve(args.size());
        for (const auto & key : args)
            func_vars.emplace_back(vars.container.at(key));

        return constraint->check(func_vars);
    }
};

using BoundConstraints = std::vector<BoundConstraint>;


class IFunctionSignature
{
public:
    virtual ~IFunctionSignature() {}
    virtual DataTypePtr check(const ColumnsWithTypeAndName & args) const = 0;
};

using FunctionSignaturePtr = std::shared_ptr<IFunctionSignature>;
using FunctionSignatures = std::vector<FunctionSignaturePtr>;


/** A signature of a function that has fixed number of arguments.
  */
struct FixedFunctionSignature : public IFunctionSignature
{
    ArgumentsDescription arguments_description;
    BoundTypeFunction return_type;
    BoundConstraints constraints;

    DataTypePtr check(const ColumnsWithTypeAndName & args) const override
    {
        Variables vars;

        size_t num_args = args.size();
        if (num_args != arguments_description.size())
            return nullptr;

        for (size_t i = 0; i < num_args; ++i)
            if (!arguments_description[i].match(args[i].type, args[i].column, vars))
                return nullptr;

        for (const BoundConstraint & constraint : constraints)
        {
            BoundConstraint expanded_constraint = constraint;
            expanded_constraint.args = expandEllipsis(vars, expanded_constraint.args);
            if (!expanded_constraint.check(vars))
                return nullptr;
        }

        BoundTypeFunction expanded_return_type = return_type;
        expanded_return_type.args = expandEllipsis(vars, expanded_return_type.args);
        return expanded_return_type.apply(vars);
    }
};


/// Generates alternative sequences of arguments.
class IVariadicArgumentsGroup
{
public:
    virtual ~IVariadicArgumentsGroup() {}

    /// Appends arguments to 'to'.
    /// The caller passes increasing 'iteration' values starting from 0 until the function returns false.
    virtual bool append(ArgumentsDescription & to, size_t iteration, size_t max_args) const = 0;
};

using ArgumentsGroupPtr = std::shared_ptr<IVariadicArgumentsGroup>;
using ArgumentsGroups = std::vector<ArgumentsGroupPtr>;


class FixedArgumentsGroup : public IVariadicArgumentsGroup
{
private:
    ArgumentsDescription args;
public:
    FixedArgumentsGroup(const ArgumentsDescription & args) : args(args) {}

    bool append(ArgumentsDescription & to, size_t iteration, size_t max_args) const override
    {
        if (iteration != 0)
            return false;

        if (to.size() + args.size() > max_args)
            return false;

        to.insert(to.end(), args.begin(), args.end());
        return true;
    }
};


class OptionalArgumentsGroup : public IVariadicArgumentsGroup
{
private:
    ArgumentsDescription args;
public:
    OptionalArgumentsGroup(const ArgumentsDescription & args) : args(args) {}

    bool append(ArgumentsDescription & to, size_t iteration, size_t max_args) const override
    {
        if (to.size() + iteration > max_args)
            return false;

        if (iteration == 0)
            return true;

        if (iteration == 1)
        {
            to.insert(to.end(), args.begin(), args.end());
            return true;
        }

        return false;
    }
};


class EllipsisArgumentsGroup : public IVariadicArgumentsGroup
{
private:
    ArgumentsDescription args;
public:
    EllipsisArgumentsGroup(const ArgumentsDescription & args) : args(args) {}

    /// Append 'args' to 'to' 1 + iteration number of times.
    bool append(ArgumentsDescription & to, size_t iteration, size_t max_args) const override
    {
        size_t repeat_count = iteration + 1;

        if (to.size() + repeat_count * args.size() > max_args)
            return false;

        for (size_t iter_num = 0; iter_num < iteration; ++iter_num)
        {
            for (const auto & arg : args)
            {
                to.emplace_back(arg);
                to.back().iteration = iter_num;
            }
        }

        return true;
    }
};


struct VariadicFunctionSignature : public IFunctionSignature
{
    ArgumentsGroups groups;
    BoundTypeFunction return_type;
    BoundConstraints constraints;

    DataTypePtr check(const ColumnsWithTypeAndName & args) const override
    {
        size_t num_args = args.size();
        size_t num_groups = groups.size();

        std::vector<size_t> iterators(groups.size());
        size_t current_iterator_idx = 0;

        while (current_iterator_idx < num_groups)
        {
            FixedFunctionSignature current_signature;

            size_t group_idx = 0;
            for (; group_idx < num_groups; ++group_idx)
                if (!groups[group_idx]->append(current_signature.arguments_description, iterators[group_idx], num_args))
                    break;

            if (group_idx == num_groups)
            {
                ++iterators[current_iterator_idx];

                current_signature.return_type = return_type;
                current_signature.constraints = constraints;

                if (DataTypePtr res = current_signature.check(args))
                    return res;
            }
            else
            {
                if (group_idx + 1 == num_groups)
                    break;

                current_iterator_idx = group_idx + 1;
                ++iterators[current_iterator_idx];

                for (size_t i = 0; i < current_iterator_idx; ++i)
                    iterators[i] = 0;
            }
        }

        return nullptr;
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

bool parseIdentifierOrEllipsis(TokenIterator & pos, std::string & res)
{
    if (pos->type == TokenType::BareWord || pos->type == TokenType::Ellipsis)
    {
        res.assign(pos->begin, pos->end);
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

template <typename ParseElem>
bool parseOptional(TokenIterator & pos, ParseElem && parse_elem)
{
    parse_elem(pos);
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

bool parseTypeFunction(TokenIterator & pos, BoundTypeFunction & res)
{
    TokenIterator begin = pos;
    std::string name;
    if (parseFunctionLikeExpression(pos, name, true,
        [&](TokenIterator & pos)
        {
            std::string elem;
            if (parseIdentifierOrEllipsis(pos, elem))   /// TODO Allow nested expressions
            {
                res.args.emplace_back(elem);
                return true;
            }
            return false;
        }))
    {
        /// Examples:
        /// - variable:      f(T) -> T
        /// - type:          f() -> UInt8
        /// - type function: f(A, B) -> LeastCommonType(A, B)

        res.func = TypeFunctionFactory::instance().tryGet(name);
        if (res.func)
            return true;

        /// Exact type.

        const auto & factory = DataTypeFactory::instance();
        if (factory.existsCanonicalFamilyName(name))
        {
            auto prev_pos = pos;
            --prev_pos;
            const std::string full_name(begin->begin, prev_pos->end);
            res.func = std::make_shared<FixedTypeFunction>(factory.get(full_name));
            return true;
        }

        /// Type variable (example: T)

        if (res.args.empty())
        {
            res.args.emplace_back(name);
            res.func = std::make_shared<IdentityTypeFunction>();
            return true;
        }

        throw Exception("Unknown type function: " + name, ErrorCodes::LOGICAL_ERROR);
    }
    return false;
}

bool parseConstraint(TokenIterator & pos, BoundConstraint & res)
{
    std::string name;
    if (parseFunctionLikeExpression(pos, name, false,
        [&](TokenIterator & pos)
        {
            std::string elem;
            if (parseIdentifierOrEllipsis(pos, elem))
            {
                res.args.emplace_back(elem);
                return true;
            }
            return false;
        }))
    {
        res.constraint = ConstraintFactory::instance().get(name);
        return true;
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

bool parseArgumentsGroup(TokenIterator & pos, ArgumentsGroupPtr & res, const ArgumentsGroupPtr & /*prev_group*/)
{
    ArgumentsDescription args;

    if (consumeToken(pos, TokenType::OpeningSquareBracket)
        && parseSimpleArgumentsDescription(pos, args)
        && consumeToken(pos, TokenType::ClosingSquareBracket))
    {
        res = std::make_shared<OptionalArgumentsGroup>(args);
        return true;
    }

    if (parseSimpleArgumentsDescription(pos, args))
    {
        res = std::make_shared<FixedArgumentsGroup>(args);
        return true;
    }

    if (consumeToken(pos, TokenType::Ellipsis))
    {
        /// TODO calculate args
        res = std::make_shared<EllipsisArgumentsGroup>(args);
        return true;
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
                    ArgumentsGroupPtr group;
                    if (parseArgumentsGroup(pos, group, res.groups.empty() ? nullptr : res.groups.back()))
                    {
                        std::cerr << "@@@@\n";
                        res.groups.emplace_back(group);
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
        && parseTypeFunction(pos, res.return_type))
    {
        std::cerr << "#\n";
        if (consumeKeyword(pos, "WHERE"))
        {
            if (!parseList(pos, false,
                [&](TokenIterator & pos)
                {
                    BoundConstraint constraint;
                    if (parseConstraint(pos, constraint))
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
