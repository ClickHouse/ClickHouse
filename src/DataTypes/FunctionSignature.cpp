#include <DataTypes/FunctionSignature.h>

#include <map>
#include <cstring>

#include <Common/FieldVisitorToString.h>
#include <Common/typeid_cast.h>
#include <Common/StringUtils.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/TokenIterator.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_FUNCTION_SIGNATURE;
    extern const int SYNTAX_ERROR;
}

namespace FunctionSignatures
{

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

    Key() = default;
    Key(std::string str, size_t index_) : name(std::move(str)), index(index_) {}

    explicit Key(std::string str) : name(std::move(str))
    {
        if (name.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty variable name");

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

    /// Increment the iteration index. Both bare (`T`) and indexed (`T1`) names participate:
    /// at iteration 0 the key is unchanged, so single-shot use like `(T : Any) -> T` still
    /// resolves to the same variable on both sides. Inside an ellipsis-bound position the
    /// captured variable becomes `T1`, `T2`, ... — distinct per repetition — which lets a
    /// signature write `(T : Array(Any), ...) -> leastSupertype(T, ...)` instead of having
    /// to introduce a `T1, ..., E` chain by hand.
    Key incrementIndex(size_t amount) const
    {
        Key res = *this;
        res.index += amount;
        return res;
    }
};


bool Value::operator==(const Value & rhs) const
{
    return value.index() == rhs.value.index()
        && ((value.index() == 0 && std::get<DataTypePtr>(value)->equals(*std::get<DataTypePtr>(rhs.value)))
            || (value.index() == 1 && std::get<Field>(value) == std::get<Field>(rhs.value)));
}

const DataTypePtr & Value::type() const
{
    return std::get<DataTypePtr>(value);
}

const Field & Value::field() const
{
    return std::get<Field>(value);
}

/// For implementation of constraints.
bool Value::isTrue() const
{
    return std::get<Field>(value).safeGet<UInt64>() == 1;
}

std::string Value::toString() const
{
    if (value.index() == 0)
    {
        const auto & t = type();
        if (!t)
            return "nullptr";
        else
            return t->getName();
    }
    else
        return applyVisitor(FieldVisitorToString(), std::get<Field>(value));
}


/** Scratch space for checking function signature.
  * Variables are either assigned or checked for equality, but never reassigned.
  */
struct Variables
{
    using Container = std::map<Key, Value>;
    Container container;

    bool has(const Key & key) const
    {
        return container.contains(key);
    }

    bool assignOrCheck(const Key & key, const DataTypePtr & var, size_t arg_num, std::string & out_reason)
    {
        if (auto it = container.find(key); it == container.end())
        {
            container.emplace(key, Value(var, arg_num));
            return true;
        }
        else
        {
            if (it->second == Value(var))
                return true;

            out_reason = "argument " + DB::toString(arg_num + 1) + " must be of type " + key.toString()
                + " that was captured as " + it->second.toString();

            if (it->second.captured_at_arg_num)
                out_reason += " at argument " + DB::toString(*it->second.captured_at_arg_num + 1);

            return false;
        }
    }

    bool assignOrCheck(const Key & key, const Field & var, size_t arg_num, std::string & out_reason)
    {
        if (auto it = container.find(key); it == container.end())
        {
            container.emplace(key, Value(var, arg_num));
            return true;
        }
        else
        {
            if (it->second == Value(var))
                return true;

            out_reason = "argument " + DB::toString(arg_num) + " must equals to " + key.toString()
                + " that was captured as " + applyVisitor(FieldVisitorToString(), var);

            if (it->second.captured_at_arg_num)
                out_reason += " at argument " + DB::toString(*it->second.captured_at_arg_num + 1);

            return false;
        }
    }

    Value get(const Key & key) const
    {
        if (auto it = container.find(key); it != container.end())
            return it->second;
        else
            throw Exception(ErrorCodes::BAD_FUNCTION_SIGNATURE, "Variable {} was not captured", key.toString());
    }

    void reset()
    {
        container.clear();
    }

    std::string toString() const
    {
        WriteBufferFromOwnString out;
        writeList(container, [&](const auto & elem){ out << elem.first.toString() << " = " << elem.second.toString(); }, [&]{ out << ", "; });
        return out.str();
    }
};


/// Matches any type.
class AnyTypeMatcher : public ITypeMatcher
{
public:
    std::string toString() const override { return "Any"; }
    bool match(const DataTypePtr &, Variables &, size_t, size_t, std::string &) const override { return true; }
    size_t getIndex() const override { return 0; }
};


/// Matches exact type.
class ExactTypeMatcher : public ITypeMatcher
{
private:
    DataTypePtr type;

public:
    explicit ExactTypeMatcher(const DataTypePtr & type_) : type(type_) {}

    std::string toString() const override
    {
        return type->getName();
    }

    bool match(const DataTypePtr & what, Variables &, size_t, size_t, std::string &) const override
    {
        return type->equals(*what);
    }

    size_t getIndex() const override { return 0; }
};


/// Matches if any of the children matches. Children are tried in order; the first match wins.
/// On a failed branch, variable assignments made by that branch are rolled back before trying the next.
class OrTypeMatcher : public ITypeMatcher
{
private:
    TypeMatchers children;

public:
    explicit OrTypeMatcher(TypeMatchers children_) : children(std::move(children_)) {}

    std::string toString() const override
    {
        WriteBufferFromOwnString out;
        writeList(children, [&](const auto & c){ out << c->toString(); }, [&]{ out << " | "; });
        return out.str();
    }

    bool match(const DataTypePtr & what, Variables & vars, size_t iteration, size_t arg_num, std::string & out_reason) const override
    {
        for (const auto & child : children)
        {
            auto saved = vars;
            std::string reason;
            if (child->match(what, vars, iteration, arg_num, reason))
                return true;
            vars = std::move(saved);
        }
        /// Outer ArgumentDescription will format "has type X that is not <our toString>", so leave out_reason empty.
        out_reason.clear();
        return false;
    }

    size_t getIndex() const override
    {
        size_t res = 0;
        for (const auto & child : children)
            res = getCommonIndex(res, child->getIndex());
        return res;
    }
};


/// Use another matcher and assign or check the matched type to the variable.
class AssignTypeMatcher : public ITypeMatcher
{
private:
    TypeMatcherPtr impl;
    Key var_key;

public:
    AssignTypeMatcher(const TypeMatcherPtr & impl_, const Key & var_key_) : impl(impl_), var_key(var_key_) {}

    std::string toString() const override
    {
        return impl
            ? var_key.toString() + " : " + impl->toString()
            : var_key.toString();
    }

    bool match(const DataTypePtr & what, Variables & vars, size_t iteration, size_t arg_num, std::string & out_reason) const override
    {
        if (impl && !impl->match(what, vars, iteration, arg_num, out_reason))
            return false;

        return vars.assignOrCheck(var_key.incrementIndex(iteration), what, arg_num, out_reason);
    }

    size_t getIndex() const override
    {
        /// The variable's own index (e.g. `1` for `K1`) participates in the
        /// ellipsis-grouping walk-back, regardless of whether a type matcher
        /// is attached. So `(K1, V1, ...)` and `(K1 : Any, V1 : Any, ...)`
        /// must both report index 1 — otherwise the bare form fails to
        /// participate in multi-element repeat groups.
        return impl ? getCommonIndex(var_key.index, impl->getIndex()) : var_key.index;
    }
};


struct ArgumentDescription
{
    bool is_const = false;
    Key argument_name;
    TypeMatcherPtr type_matcher;

    bool match(const DataTypePtr & type, const ColumnPtr & column, Variables & vars, size_t iteration,
        size_t arg_num, std::string & out_reason) const
    {
        /// `column == nullptr` means the caller has no column information (e.g.
        /// `getReturnTypeImpl(DataTypes)` reached us through the legacy types-only
        /// path). In that mode we can't decide constness, so skip the check and
        /// the const-value capture — both would reject every const-constrained
        /// signature on this path otherwise.
        if (is_const && column && !isColumnConst(*column))
        {
            out_reason = "argument " + DB::toString(arg_num + 1) + (argument_name.name.empty() ? "" : " (" + argument_name.name + ")")
                + " must be " + toString() + ", but it is not constant";
            return false;
        }

        if (!argument_name.name.empty())
        {
            auto key = argument_name.incrementIndex(iteration);
            if (is_const && column && !vars.assignOrCheck(key, typeid_cast<const ColumnConst &>(*column).getField(), arg_num, out_reason))
            {
                return false;
            }
        }

        if (!type_matcher->match(type, vars, iteration, arg_num, out_reason))
        {
            out_reason = "argument " + DB::toString(arg_num + 1) + (argument_name.name.empty() ? "" : " (" + argument_name.name + ")")
                + " has type " + type->getName() + " that is not " + type_matcher->toString()
                + (out_reason.empty() ? "" : ": " + out_reason);
            return false;
        }

        return true;
    }

    /// Extract common index of variables participating in expression.
    size_t getIndex() const
    {
        return getCommonIndex(argument_name.index, type_matcher->getIndex());
    }

    std::string toString() const
    {
        WriteBufferFromOwnString out;
        if (is_const)
            out << "const ";
        if (!argument_name.name.empty())
            out << argument_name.toString() << " ";
        out << type_matcher->toString();
        return out.str();
    }
};

using ArgumentsDescription = std::vector<ArgumentDescription>;

struct ArgumentsGroup
{
    ArgumentsDescription elems;
    enum Type
    {
        NoType,
        Fixed,      /// As is.
        Optional,   /// Zero or one time.
        Ellipsis    /// Zero or any number of times.
    } type = NoType;

    bool match(const ColumnsWithTypeAndName & args, Variables & vars, size_t offset, size_t iteration, std::string & out_reason) const
    {
        size_t size = elems.size();

        if (size + offset > args.size())
        {
            out_reason = "too few arguments (" + DB::toString(args.size()) + "), must be at least " + DB::toString(size + offset);
            return false;
        }

        for (size_t i = 0; i < size; ++i)
        {
            const auto & col = args[offset + i];
            if (!elems[i].match(col.type, col.column, vars, iteration, offset + i, out_reason))
                return false;
        }
        return true;
    }

    std::string toString() const
    {
        WriteBufferFromOwnString out;

        if (type == Ellipsis)
        {
            out << "...";
            return out.str();
        }

        if (type == Optional)
            out << "[";

        writeList(elems, [&](const auto & elem){ out << elem.toString(); }, [&]{ out << ", "; });

        if (type == Optional)
            out << "]";

        return out.str();
    }
};

using ArgumentsGroups = std::vector<ArgumentsGroup>;

class VariadicArguments
{
public:
    ArgumentsGroups groups;

    bool match(const ColumnsWithTypeAndName & args, Variables & vars, std::optional<size_t> & ellipsis_size, std::string & out_reason) const
    {
        return matchAt(args, vars, 0, 0, 0, ellipsis_size, out_reason);
    }

    std::string toString() const
    {
        WriteBufferFromOwnString out;
        writeList(groups, [&](const auto & elem){ out << elem.toString(); }, [&]{ out << ", "; });
        return out.str();
    }

private:
    bool matchAt(const ColumnsWithTypeAndName & args, Variables & vars,
        size_t group_offset, size_t args_offset, size_t iteration,
        std::optional<size_t> & ellipsis_size,
        std::string & out_reason) const
    {
        /// Tweak for better out_reason message: if all groups are fixed and one group is ellipsis: determine ellipsis size.
        if (args_offset == 0 && group_offset == 0)
        {
            size_t num_ellipsis_groups = 0;
            size_t num_fixed_groups = 0;
            size_t num_fixed_args = 0;
            size_t num_args_in_ellipsis_group = 0;

            for (const auto & group : groups)
            {
                if (group.type == ArgumentsGroup::Fixed)
                {
                    ++num_fixed_groups;
                    num_fixed_args += group.elems.size();
                }
                if (group.type == ArgumentsGroup::Ellipsis)
                {
                    ++num_ellipsis_groups;
                    num_args_in_ellipsis_group += group.elems.size();
                }
            }

            if (num_ellipsis_groups == 1 && num_fixed_groups + num_ellipsis_groups == groups.size())
            {
                if (args.size() < num_fixed_args)
                {
                    out_reason = "too few arguments (" + DB::toString(args.size()) + "), must be at least " + DB::toString(num_fixed_args);
                    return false;
                }

                size_t expected_num_args_in_ellipsis = args.size() - num_fixed_args;

                if (expected_num_args_in_ellipsis % num_args_in_ellipsis_group != 0)
                {
                    out_reason = "number of arguments (" + DB::toString(args.size()) + ") doesn't match, expected "
                        + DB::toString(num_fixed_args) + " + n * " + DB::toString(num_args_in_ellipsis_group);
                    return false;
                }

                ellipsis_size = expected_num_args_in_ellipsis / num_args_in_ellipsis_group;
            }
        }

        auto new_vars = vars;
        if (matchImpl(args, new_vars, group_offset, args_offset, iteration, ellipsis_size, out_reason))
        {
            vars = new_vars;
            out_reason.clear();
            return true;
        }
        return false;
    }

    bool matchImpl(const ColumnsWithTypeAndName & args, Variables & vars,
        size_t group_offset, size_t args_offset, size_t iteration,
        std::optional<size_t> & ellipsis_size,
        std::string & out_reason) const
    {
        /// All groups have matched all arguments.
        if (group_offset == groups.size() && args_offset == args.size())
            return true;

        /// Not enough groups to match all arguments.
        if (group_offset >= groups.size())
        {
            out_reason = "too many arguments (" + DB::toString(args.size()) + ")";
            return false;
        }

        /// All arguments have been matched but there are more groups.
        if (args_offset > args.size())
        {
            out_reason = "too few arguments (" + DB::toString(args.size()) + ")";
            return false;
        }

        const ArgumentsGroup & group = groups[group_offset];
        size_t group_size = group.elems.size();
        switch (group.type)
        {
            case ArgumentsGroup::Fixed:
                return group.match(args, vars, args_offset, 0, out_reason)
                    && matchAt(args, vars, group_offset + 1, args_offset + group_size, iteration, ellipsis_size, out_reason);

            case ArgumentsGroup::Optional:
                /// Skip group or match group and continue.
                return matchAt(args, vars, group_offset + 1, args_offset, iteration, ellipsis_size, out_reason)
                    || (group.match(args, vars, args_offset, 0, out_reason)
                        && matchAt(args, vars, group_offset + 1, args_offset + group_size, iteration, ellipsis_size, out_reason));

            case ArgumentsGroup::Ellipsis:
            {
                if (ellipsis_size)
                {
                    for (size_t i = 0; i < *ellipsis_size; ++i)
                        if (!group.match(args, vars, args_offset + group_size * i, iteration + 1 + i, out_reason))
                            return false;
                    return matchAt(args, vars, group_offset + 1, args_offset + group_size * *ellipsis_size, iteration, ellipsis_size, out_reason);
                }
                else
                {
                    size_t repeat_count = 0;
                    while (true)
                    {
                        /// Ellipsis is ended, match from next group.
                        ellipsis_size = repeat_count;
                        if (matchAt(args, vars, group_offset + 1, args_offset, iteration, ellipsis_size, out_reason))
                            return true;
                        else
                            ellipsis_size.reset();

                        /// Repeat group for ellipsis.
                        if (!group.match(args, vars, args_offset, iteration + repeat_count + 1, out_reason))
                            return false;

                        args_offset += group_size;
                        ++repeat_count;
                    }
                }
            }
            case ArgumentsGroup::NoType:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong type of ArgumentsGroup");
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong type of ArgumentsGroup");
    }
};


/** Part of expression tree that contains type functions, variables and constants.
  */
class ITypeExpression
{
public:
    virtual ~ITypeExpression() = default;
    virtual Value apply(const Variables & context, size_t iteration, std::optional<size_t> ellipsis_size) const = 0;

    /// Extract common index of variables participating in expression.
    virtual size_t getIndex() const = 0;

    virtual std::string toString() const = 0;
};

using TypeExpressionPtr = std::shared_ptr<ITypeExpression>;
using TypeExpressions = std::vector<TypeExpressionPtr>;

/// Takes no arguments and returns a value of variable from context.
class VariableTypeExpression : public ITypeExpression
{
private:
    Key key;
public:
    explicit VariableTypeExpression(const Key & key_) : key(key_) {}

    Value apply(const Variables & context, size_t iteration, std::optional<size_t>) const override
    {
        return context.get(key.incrementIndex(iteration));
    }

    size_t getIndex() const override
    {
        return key.index;
    }

    std::string toString() const override
    {
        return key.toString();
    }
};

/// Takes zero arguments and returns pre-determined value. Lets us treat a value as a function of zero arguments.
class ConstantTypeExpression : public ITypeExpression
{
private:
    Value res;
public:
    explicit ConstantTypeExpression(const Value & res_) : res(res_) {}

    Value apply(const Variables &, size_t, std::optional<size_t>) const override
    {
        return res;
    }

    size_t getIndex() const override
    {
        return 0;
    }

    std::string toString() const override
    {
        return res.toString();
    }
};

class TypeExpressionTree : public ITypeExpression
{
private:
    TypeFunctionPtr func;
    TypeExpressions children;   /// nullptr child means ellipsis.

public:
    TypeExpressionTree(const TypeFunctionPtr & func_, const TypeExpressions & children_) : func(func_), children(children_) {}

    Value apply(const Variables & context, size_t iteration, std::optional<size_t> ellipsis_size) const override
    {
        Values args;

        /// Accumulate a group of children to repeat when ellipsis is encountered. Children
        /// participate as long as they belong to the same "iteration index": a bare variable
        /// like `T` or a constant carries index 0; an indexed variable like `T1` carries
        /// index 1, etc. When we hit a child whose index differs from the prior group's
        /// index we restart the group with just that child. This mirrors the same walk-back
        /// rule used when parsing argument groups, so `leastSupertype(T, ...)` and the older
        /// `leastSupertype(T1, ..., E)` form both produce the same expansion.
        TypeExpressions group_to_repeat;
        size_t prev_index = 0;

        for (const auto & child : children)
        {
            if (child)
            {
                args.emplace_back(child->apply(context, iteration, ellipsis_size));

                size_t current_index = child->getIndex();
                if (!group_to_repeat.empty() && prev_index != current_index)
                    group_to_repeat.clear();
                group_to_repeat.emplace_back(child);
                prev_index = current_index;
            }
            else    /// Ellipsis.
            {
                if (group_to_repeat.empty())
                    throw Exception(ErrorCodes::BAD_FUNCTION_SIGNATURE, "No group to repeat in type function");

                if (!ellipsis_size)
                    throw Exception(ErrorCodes::BAD_FUNCTION_SIGNATURE, "There is an ellipsis in type expression, but no ellipsis was matched as arguments");

                for (size_t repeat_iteration = 0; repeat_iteration < *ellipsis_size; ++repeat_iteration)
                    for (const auto & expr : group_to_repeat)
                        args.emplace_back(expr->apply(context, 1 + repeat_iteration, ellipsis_size));
            }
        }
        return func->apply(args);
    }

    size_t getIndex() const override
    {
        size_t res = 0;
        for (const auto & child : children)
            if (child)
                res = getCommonIndex(res, child->getIndex());
        return res;
    }

    std::string toString() const override
    {
        WriteBufferFromOwnString out;
        out << func->name() << "(";
        writeList(children, [&](const auto & elem){ out << (elem ? elem->toString() : "..."); }, [&]{ out << ", "; });
        out << ")";
        return out.str();
    }
};


class IFunctionSignatureImpl
{
public:
    virtual ~IFunctionSignatureImpl() = default;
    virtual DataTypePtr check(const ColumnsWithTypeAndName & args, Variables & vars, std::string & out_reason) const = 0;
    virtual std::string toString() const = 0;
};

using FunctionSignatureImplPtr = std::shared_ptr<IFunctionSignatureImpl>;
using FunctionSignatureImpls = std::vector<FunctionSignatureImplPtr>;


struct VariadicFunctionSignatureImpl : public IFunctionSignatureImpl
{
    std::string name;
    VariadicArguments arguments_description;
    TypeExpressionPtr return_type;
    TypeExpressions constraints;

    DataTypePtr check(const ColumnsWithTypeAndName & args, Variables & vars, std::string & out_reason) const override
    {
        /// Apply type matchers and assign variables.

        std::optional<size_t> ellipsis_size;
        if (!arguments_description.match(args, vars, ellipsis_size, out_reason))
            return nullptr;

        /// Check constraints against variables.

        for (const TypeExpressionPtr & constraint : constraints)
        {
            if (!constraint->apply(vars, 0, ellipsis_size).isTrue())
            {
                out_reason = "constraint " + constraint->toString() + " is not satisfied";
                return nullptr;
            }
        }

        return std::get<DataTypePtr>(return_type->apply(vars, 0, ellipsis_size).value);
    }

    std::string toString() const override
    {
        WriteBufferFromOwnString out;
        out << (name.empty() ? "f" : name) << "(" << arguments_description.toString() << ") -> " << return_type->toString();

        if (!constraints.empty())
        {
            out << " WHERE ";
            writeList(constraints, [&](const auto & elem){ out << elem->toString(); }, [&]{ out << ", "; });
        }

        return out.str();
    }
};


struct AlternativeFunctionSignatureImpl : public IFunctionSignatureImpl
{
    FunctionSignatureImpls alternatives;

    DataTypePtr check(const ColumnsWithTypeAndName & args, Variables & vars, std::string & out_reason) const override
    {
        if (alternatives.size() == 1)
            return alternatives.front()->check(args, vars, out_reason);

        std::vector<std::string> reasons;

        for (const auto & alternative : alternatives)
        {
            vars.reset();
            reasons.emplace_back();
            if (DataTypePtr res = alternative->check(args, vars, reasons.back()))
                return res;
        }

        WriteBufferFromOwnString out;
        out << "None of the alternative function signatures matched.\n";
        size_t num_alternatives = alternatives.size();
        for (size_t i = 0; i < num_alternatives; ++i)
            out << "Variant " << alternatives[i]->toString() << " doesn't match because " << reasons[i] << ".\n";
        out_reason = out.str();

        return nullptr;
    }

    std::string toString() const override
    {
        WriteBufferFromOwnString out;
        writeList(alternatives, [&](const auto & elem){ out << elem->toString(); }, [&]{ out << "\n OR "; });
        return out.str();
    }
};


/** Grammar (kinda):
  *
  * simple_signature ::= identifier '(' arguments_list ')' '->' type_func
  * simple_signature_with_constraints ::= simple_signature ('WHERE' constraints_list)?
  * alternative_signature ::= simple_signature_with_constraints ('OR' simple_signature_with_constraints)?
  *
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
        ++pos;
        return true;
    }
    return false;
}

bool consumeToken(TokenIterator & pos, TokenType type)
{
    if (pos->type == type)
    {
        ++pos;
        return true;
    }
    return false;
}

/// Detect `...` formed from three `Dot` tokens.
bool consumeEllipsis(TokenIterator & pos)
{
    TokenIterator probe = pos;
    if (probe->type != TokenType::Dot) return false;
    ++probe;
    if (probe->type != TokenType::Dot) return false;
    ++probe;
    if (probe->type != TokenType::Dot) return false;
    ++probe;
    pos = probe;
    return true;
}

bool consumeKeyword(TokenIterator & pos, const std::string & keyword)
{
    if (pos->type == TokenType::BareWord)
    {
        if (pos->size() != keyword.size() || strncasecmp(pos->begin, keyword.data(), pos->size()) != 0)
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
        [](TokenIterator & inner)
        {
            return consumeToken(inner, TokenType::Comma);
        })
        && consumeToken(pos, TokenType::ClosingRoundBracket);
}

bool typeFamilyExists(const std::string & family_name)
{
    /// existsCanonicalFamilyName is gone in modern code; emulate by trying to construct an empty-parameter type.
    return DataTypeFactory::instance().tryGet(family_name) != nullptr
        || DataTypeFactory::instance().tryGet(family_name, ASTPtr{}) != nullptr;
}

bool parseTypeExpression(TokenIterator & pos, TypeExpressionPtr & res)
{
    /// Integer literal — used inside return-type expressions to pass numeric arguments
    /// to type functions, e.g. `DateTime64(max(scaleOf(T), 3))`.
    if (pos->type == TokenType::Number)
    {
        ReadBufferFromMemory buf(pos->begin, pos->end - pos->begin);
        UInt64 value = 0;
        if (tryReadIntText(value, buf) && buf.eof())
        {
            res = std::make_shared<ConstantTypeExpression>(Value(Field(value)));
            ++pos;
            return true;
        }
    }

    /// String literal — used inside return-type expressions, e.g. `NamedField('origin', UInt64)`.
    if (pos->type == TokenType::StringLiteral)
    {
        ReadBufferFromMemory buf(pos->begin, pos->end - pos->begin);
        String value;
        readQuotedStringWithSQLStyle(value, buf);
        res = std::make_shared<ConstantTypeExpression>(Value(Field(value)));
        ++pos;
        return true;
    }

    TokenIterator begin = pos;
    std::string name;
    TypeExpressions children;
    if (parseFunctionLikeExpression(pos, name, true,
        [&](TokenIterator & inner)
        {
            TypeExpressionPtr elem;

            /// Named-field shorthand: `<field_name> <type_expr>` inside a function-call arg list
            /// desugars to `NamedField('<field_name>', <type_expr>)`. Used by Tuple, e.g.
            /// `Tuple(origin UInt64, destination UInt64)`. Restricted to inside parens so that
            /// top-level `T OR ...` and `UInt64 OR ...` don't get misparsed.
            if (inner->type == TokenType::BareWord)
            {
                auto probe = inner;
                ++probe;
                if (probe->type == TokenType::BareWord)
                {
                    auto saved = inner;
                    std::string field_name;
                    if (parseIdentifier(inner, field_name))
                    {
                        TypeExpressionPtr inner_expr;
                        if (parseTypeExpression(inner, inner_expr))
                        {
                            elem = std::make_shared<TypeExpressionTree>(
                                TypeFunctionFactory::instance().get("NamedField"),
                                TypeExpressions{
                                    std::make_shared<ConstantTypeExpression>(Value(Field(field_name))),
                                    inner_expr,
                                });
                            children.emplace_back(elem);
                            return true;
                        }
                        inner = saved;
                    }
                }
            }

            if (parseTypeExpression(inner, elem))
            {
                children.emplace_back(elem);
                return true;
            }
            if (consumeEllipsis(inner))
            {
                children.emplace_back(nullptr);
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
            res = std::make_shared<TypeExpressionTree>(func, children);
            return true;
        }

        /// Exact type.

        const auto & factory = DataTypeFactory::instance();
        if (typeFamilyExists(name))
        {
            auto prev_pos = pos;
            --prev_pos;
            const std::string full_name(begin->begin, prev_pos->end);
            try
            {
                res = std::make_shared<ConstantTypeExpression>(Value(factory.get(full_name)));
            }
            catch (Exception & e)
            {
                e.addMessage("in expression " + full_name);
                throw;
            }
            return true;
        }

        /// Type variable (example: T).

        if (children.empty())
        {
            res = std::make_shared<VariableTypeExpression>(Key(name));
            return true;
        }

        throw Exception(ErrorCodes::BAD_FUNCTION_SIGNATURE, "Unknown type function: {}", name);
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
        [&](TokenIterator & inner)
        {
            /// String literal child — used by matchers like AggregateFunction('groupBitmap', T)
            /// to require a specific name. Wrap in a placeholder matcher that the parent reads.
            if (inner->type == TokenType::StringLiteral)
            {
                ReadBufferFromMemory buf(inner->begin, inner->end - inner->begin);
                String value;
                readQuotedStringWithSQLStyle(value, buf);
                args.emplace_back(makeStringLiteralMatcher(std::move(value)));
                ++inner;
                return true;
            }

            /// Integer literal child — used by matchers like `TupleOfSize(3)` that take a
            /// numeric constant. We carry the literal as a placeholder string (matching how
            /// `StringLiteral` is handled); the parent matcher parses it back to an integer.
            if (inner->type == TokenType::Number)
            {
                args.emplace_back(makeStringLiteralMatcher(std::string(inner->begin, inner->end - inner->begin)));
                ++inner;
                return true;
            }

            /// Parenthesized matcher list `(m1, m2, ...)` — used by matchers like
            /// Function((Arg1, Arg2), Result) that need a list of matchers in a single arg slot.
            /// A trailing `...` is captured as an ellipsis-marker matcher; parent matchers
            /// that support variadic positions (e.g. `Function`) consume it during construction.
            if (inner->type == TokenType::OpeningRoundBracket)
            {
                auto saved = inner;
                ++inner;
                TypeMatchers list_elems;
                if (parseList(inner, true,
                    [&](TokenIterator & it)
                    {
                        if (consumeEllipsis(it))
                        {
                            list_elems.emplace_back(makeEllipsisMarkerMatcher());
                            return true;
                        }
                        TypeMatcherPtr elem;
                        if (!parseTypeMatcher(it, elem))
                            return false;
                        list_elems.emplace_back(elem);
                        return true;
                    },
                    [](TokenIterator & it) { return consumeToken(it, TokenType::Comma); })
                    && consumeToken(inner, TokenType::ClosingRoundBracket))
                {
                    args.emplace_back(makeListMatcher(std::move(list_elems)));
                    return true;
                }
                inner = saved;
            }

            TypeMatcherPtr elem;
            if (parseTypeMatcher(inner, elem))
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
        if (typeFamilyExists(name))
        {
            auto prev_pos = pos;
            --prev_pos;
            const std::string full_name(begin->begin, prev_pos->end);
            try
            {
                res = std::make_shared<ExactTypeMatcher>(factory.get(full_name));
            }
            catch (Exception & e)
            {
                e.addMessage("in expression " + full_name);
                throw;
            }
            return true;
        }

        /// Type variable (example: T).

        if (args.empty() && name.size() < 3)    /// Size limit for better disambiguation with type matchers.
        {
            res = std::make_shared<AssignTypeMatcher>(nullptr, Key(name));
            return true;
        }

        throw Exception(ErrorCodes::BAD_FUNCTION_SIGNATURE, "Unknown type matcher: {}", name);
    }
    return false;
}

/// One or more simple matchers separated by `|`. The `|` operator binds tighter than `T :`
/// so `T : Float | Decimal` reads as `T : (Float | Decimal)`.
bool parseOrTypeMatcher(TokenIterator & pos, TypeMatcherPtr & res)
{
    if (!parseSimpleTypeMatcher(pos, res))
        return false;

    TypeMatchers alternatives;
    while (consumeToken(pos, TokenType::PipeMark))
    {
        TypeMatcherPtr next;
        if (!parseSimpleTypeMatcher(pos, next))
            return false;
        if (alternatives.empty())
            alternatives.emplace_back(res);
        alternatives.emplace_back(next);
    }

    if (!alternatives.empty())
        res = std::make_shared<OrTypeMatcher>(std::move(alternatives));
    return true;
}

bool parseTypeMatcher(TokenIterator & pos, TypeMatcherPtr & res)
{
    /// Matcher
    /// Matcher(...)
    /// Matcher | Matcher | ...
    /// T : Matcher
    /// T : Matcher(...)
    /// T : Matcher | Matcher | ...

    auto next_pos = pos;
    ++next_pos;

    if (consumeToken(next_pos, TokenType::Colon))
    {
        std::string var_name;
        if (!parseIdentifier(pos, var_name))
            return false;

        if (!parseOrTypeMatcher(next_pos, res))
            return false;

        pos = next_pos;
        res = std::make_shared<AssignTypeMatcher>(res, Key(var_name));
        return true;
    }
    return parseOrTypeMatcher(pos, res);
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
        res.argument_name = Key(identifier);
    }

    return parseTypeMatcher(pos, res.type_matcher);
}

bool parseSimpleArgumentsDescription(TokenIterator & pos, ArgumentsDescription & res)
{
    return parseList(pos, false,
        [&](TokenIterator & inner)
        {
            ArgumentDescription arg;
            if (!parseSimpleArgumentDescription(inner, arg))
                return false;
            res.emplace_back(arg);
            return true;
        },
        [](TokenIterator & inner)
        {
            return consumeToken(inner, TokenType::Comma);
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

    if (consumeEllipsis(pos))
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
            /// Walk backwards through the preceding fixed group to figure out which
            /// trailing elements form the repeated unit. The rule: while consecutive
            /// elements share the same non-zero variable index, they're part of the
            /// repeated unit. An element with index 0 (an exact type or matcher with
            /// no capture) terminates the walk — it's included in the unit if it
            /// directly precedes the indexed run (so patterns like
            /// `(UInt8, V1 : Any, ...)` repeat the pair `(UInt8, V1)`), but not if
            /// the walk has already crossed an index boundary.
            size_t prev_index = 0;
            for (auto it = prev_group.elems.rbegin(); it != prev_group.elems.rend(); ++it)
            {
                size_t current_index = it->getIndex();
                if (!current_index)
                {
                    /// Prepend so the original order is preserved when the leading
                    /// element of the repeating unit has index 0.
                    res.elems.emplace(res.elems.begin(), *it);
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
            throw Exception(ErrorCodes::BAD_FUNCTION_SIGNATURE, "Bad arguments group before ellipsis");
    }

    return false;
}

bool parseVariadicFunctionSignature(TokenIterator & pos, VariadicFunctionSignatureImpl & res)
{
    /// Allow signatures with no function name, starting directly with '('.
    auto parse_args_in_parens = [&](TokenIterator & inner)
    {
        return parseList(inner, true,
            [&](TokenIterator & inner2)
            {
                ArgumentsGroup group;
                if (parseArgumentsGroup(inner2, group, res.arguments_description.groups.empty() ? ArgumentsGroup() : res.arguments_description.groups.back()))
                {
                    res.arguments_description.groups.emplace_back(group);
                    return true;
                }
                return false;
            },
            [](TokenIterator & inner2)
            {
                return consumeToken(inner2, TokenType::Comma);
            });
    };

    bool args_parsed = false;
    if (pos->type == TokenType::OpeningRoundBracket)
    {
        ++pos;
        args_parsed = parse_args_in_parens(pos) && consumeToken(pos, TokenType::ClosingRoundBracket);
    }
    else
    {
        args_parsed = parseFunctionLikeExpression(pos, res.name, true,
            [&](TokenIterator & inner)
            {
                return parse_args_in_parens(inner);
            });
    }

    if (args_parsed
        && consumeToken(pos, TokenType::Arrow)
        && parseTypeExpression(pos, res.return_type))
    {
        if (consumeKeyword(pos, "WHERE"))
        {
            if (!parseList(pos, false,
                [&](TokenIterator & inner)
                {
                    TypeExpressionPtr constraint;
                    if (parseTypeExpression(inner, constraint))
                    {
                        res.constraints.emplace_back(constraint);
                        return true;
                    }
                    return false;
                },
                [](TokenIterator & inner)
                {
                    return consumeToken(inner, TokenType::Comma);
                }))
            {
                return false;
            }
        }

        return true;
    }

    return false;
}

bool parseAlternativeFunctionSignature(TokenIterator & pos, FunctionSignatureImplPtr & res)
{
    auto signature = std::make_shared<AlternativeFunctionSignatureImpl>();
    if (parseList(pos, false,
        [&](TokenIterator & inner)
        {
            auto alternative = std::make_shared<VariadicFunctionSignatureImpl>();
            if (parseVariadicFunctionSignature(inner, *alternative))
            {
                signature->alternatives.emplace_back(alternative);
                return true;
            }
            return false;
        },
        [](TokenIterator & inner)
        {
            return consumeKeyword(inner, "OR");
        }))
    {
        res = std::move(signature);
        return true;
    }

    return false;
}

bool parseFunctionSignature(TokenIterator & pos, FunctionSignatureImplPtr & res)
{
    return parseAlternativeFunctionSignature(pos, res);
}

}


FunctionSignature::FunctionSignature(const std::string & str)
{
    static std::once_flag init;
    std::call_once(init, []
    {
        FunctionSignatures::registerTypeMatchers();
        FunctionSignatures::registerTypeFunctions();
    });

    Tokens tokens(str.data(), str.data() + str.size());
    TokenIterator it(tokens);

    try
    {
        if (!FunctionSignatures::parseFunctionSignature(it, impl) || it->type != TokenType::EndOfStream)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Cannot parse function signature: {}", str);
    }
    catch (Exception & e)
    {
        e.addMessage("Cannot parse function signature: " + str);
        throw;
    }
}

DataTypePtr FunctionSignature::check(const ColumnsWithTypeAndName & args, std::string & out_reason) const
{
    FunctionSignatures::Variables vars;
    return impl->check(args, vars, out_reason);
}

std::string FunctionSignature::toString() const
{
    return impl ? impl->toString() : std::string{};
}

}
