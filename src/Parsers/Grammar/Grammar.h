#pragma once

#include <cstdint>
#include <iostream>
#include <vector>
#include <stack>
#include <tuple>
#include <memory>
#include <strings.h>
#include <Common/StringUtils/StringUtils.h>
#include <Parsers/TokenIterator.h>


#define APPLY_FOR_NODES(M) \
    M(NoType) \
    M(Token) \
    \
    M(ExpressionList) \
    M(ExpressionElement) \
    \
    M(Identifier) \
    M(CompoundIdentifier) \
    M(Literal) \
    M(Function) \
    \
    M(SelectQuery) \
    M(OrderDirection) \
    M(OrderExpression) \
    M(OrderExpressionList) \
    M(LimitExpression) \
    M(CollateModifier) \
    \
    M(ArraySubscript) \
    M(TupleSubscript) \
    M(UnaryMinus) \
    M(Multiplicative) \
    M(Interval) \
    M(Additive) \
    M(Concat) \
    M(Between) \
    M(Comparison) \
    M(NullCheck) \
    M(LogicalNot) \
    M(LogicalAnd) \
    M(LogicalOr) \
    M(Conditional) \
    M(Lambda) \


namespace DB
{

namespace Grammar
{

enum class NodeType
{
#define M(NAME) NAME,
    APPLY_FOR_NODES(M)
#undef M
};

const char * getNodeName(NodeType type)
{
    switch (type)
    {
#define M(NAME) \
        case NodeType::NAME: return #NAME;
    APPLY_FOR_NODES(M)
#undef M
    }
    __builtin_unreachable();
}


struct Node
{
    NodeType type = NodeType::NoType;
    Token token;    /// For leaf nodes;
    std::vector<Node> children;

    void print(size_t indent = 0) const
    {
        std::cerr << std::string(indent, ' ');
        std::cerr << getNodeName(type);
        if (type == NodeType::Token)
        {
            std::cerr << " " << getTokenName(token.type) << ": ";
            std::cerr.write(token.begin, token.size());
        }
        std::cerr << '\n';

        for (const auto & child : children)
            child.print(indent + 1);
    }
};

template <typename Derived>
struct Helper
{
    bool match(TokenIterator & it, Node & node) const
    {
        TokenIterator saved_it = it;
        Node nested_node;
        if (static_cast<const Derived &>(*this).matchImpl(it, nested_node))
        {
            node.type = nested_node.type;
            if (nested_node.type != NodeType::NoType)
            {
                node.children.emplace_back(std::move(nested_node));
            }
            else if (!nested_node.children.empty())
            {
                for (auto & child : nested_node.children)
                    node.children.emplace_back(std::move(child));
            }
            nested_node.type = NodeType::NoType;
            return true;
        }
        it = saved_it;
        return false;
    }
};

template <typename Grammar>
struct Tag : Helper<Tag<Grammar>>
{
    NodeType type;
    Grammar grammar;

    Tag(NodeType type_, Grammar grammar_) : type(type_), grammar(std::move(grammar_)) {}

    bool matchImpl(TokenIterator & it, Node & node) const
    {
        std::cerr << "Parsing " << getNodeName(type) << ": " << std::string(it->begin, it->end) << "\n";
        if (grammar.matchImpl(it, node))
        {
            std::cerr << "Parsed " << getNodeName(type);
            std::cerr << "\n";
            node.type = type;
            return true;
        }
        return false;
    }
};

struct Keyword : Helper<Keyword>
{
    const char * const s;
    const size_t size;
    Keyword(const char * s_) : s(s_), size(strlen(s)) {}

    bool matchImpl(TokenIterator & it, Node & node) const
    {
        if (it->type == TokenType::BareWord
            && it->size() == size
            && 0 == strncasecmp(it->begin, s, size))
        {
            node.type = NodeType::Token;
            node.token = *it;
            ++it;
            return true;
        }
        return false;
    }
};

struct Nothing
{
    bool match(TokenIterator &, Node &) const
    {
        return false;
    }
};

template <TokenType type>
struct Just : Helper<Just<type>>
{
    bool matchImpl(TokenIterator & it, Node & node) const
    {
        if (it->type == type)
        {
            node.type = NodeType::Token;
            node.token = *it;
            ++it;
            return true;
        }
        return false;
    }
};

template <typename... Grammars>
struct Sequence : Helper<Sequence<Grammars...>>
{
    std::tuple<Grammars...> grammars;
    Sequence(Grammars... grammars_) : grammars(std::move(grammars_)...) {}

    bool matchImpl(TokenIterator & it, Node & node) const
    {
        return std::apply([&](auto... grammar){ return ( grammar.match(it, node) && ... ); }, grammars);
    }
};

template <typename... Grammars>
struct Alternative : Helper<Alternative<Grammars...>>
{
    std::tuple<Grammars...> grammars;
    Alternative(Grammars... grammars_) : grammars(std::move(grammars_)...) {}

    bool matchImpl(TokenIterator & it, Node & node) const
    {
        return std::apply([&](auto... grammar){ return ( grammar.match(it, node) || ... ); }, grammars);
    }
};

template <typename Grammar>
struct Optional : Helper<Optional<Grammar>>
{
    Grammar grammar;
    Optional(Grammar grammar_) : grammar(std::move(grammar_)) {}

    bool matchImpl(TokenIterator & it, Node & node) const
    {
        grammar.match(it, node);
        return true;
    }
};

template <typename Grammar>
struct ZeroOrMore : Helper<ZeroOrMore<Grammar>>
{
    Grammar grammar;
    ZeroOrMore(Grammar grammar_) : grammar(std::move(grammar_)) {}

    bool matchImpl(TokenIterator & it, Node & node) const
    {
        while (grammar.match(it, node))
            ;
        return true;
    }
};

template <typename Grammar>
struct OneOrMore : Helper<OneOrMore<Grammar>>
{
    Grammar grammar;
    OneOrMore(Grammar grammar_) : grammar(std::move(grammar_)) {}

    bool matchImpl(TokenIterator & it, Node & node) const
    {
        if (!grammar.match(it, node))
            return false;

        while (grammar.match(it, node))
            ;
        return true;
    }
};

template <typename Element, typename Delimiter>
struct DelimitedSequence : Helper<DelimitedSequence<Element, Delimiter>>
{
    Element element;
    Delimiter delimiter;
    DelimitedSequence(Element element_, Delimiter delimiter_) : element(std::move(element_)), delimiter(std::move(delimiter_)) {}

    bool matchImpl(TokenIterator & it, Node & node) const
    {
        if (!element.match(it, node))
            return false;

        while (delimiter.match(it, node) && element.match(it, node))
            ;
        return true;
    }
};

template <typename Element, typename Delimiter>
struct ProperDelimitedSequence : Helper<ProperDelimitedSequence<Element, Delimiter>>
{
    Element element;
    Delimiter delimiter;
    ProperDelimitedSequence(Element element_, Delimiter delimiter_) : element(std::move(element_)), delimiter(std::move(delimiter_)) {}

    bool matchImpl(TokenIterator & it, Node & node) const
    {
        if (!(element.match(it, node) && delimiter.match(it, node) && element.match(it, node)))
            return false;

        while (delimiter.match(it, node) && element.match(it, node))
            ;
        return true;
    }
};

struct Indirect
{
    using Impl = std::function<bool(TokenIterator &, Node &)>;
    std::shared_ptr<Impl> impl = std::make_shared<Impl>();

    Indirect() = default;

    template <typename Grammar>
    Indirect(Grammar grammar)
    {
        *this = std::move(grammar);
    }

    template <typename Grammar>
    Indirect & operator=(Grammar grammar)
    {
        *impl = [grammar = std::move(grammar)](TokenIterator & it, Node & node) { return grammar.match(it, node); };
        return *this;
    }

    bool match(TokenIterator & it, Node & node) const
    {
        return (*impl)(it, node);
    }
};


enum class OperatorType : uint8_t
{
    Infix,          /// Example: a + b
    Prefix,         /// Example: -a
    InfixSuffix,    /// Example: a[b]
    PrefixInfix,    /// Example: BETWEEN a AND b
    PrefixSuffix,   /// Example: INTERVAL a SECOND
    Ternary,        /// Example: a ? b : c
};

struct Operator
{
    Indirect main;
    Indirect second;
    NodeType tag;
    int priority;
    OperatorType type;

    template <typename Main, typename Second = Nothing>
    Operator(int priority_, OperatorType type_, NodeType tag_, Main main_, Second second_ = {})
        : main(std::move(main_)), second(std::move(second_)), tag(tag_), priority(priority_), type(type_) {}
};

struct MatchedOperator
{
    NodeType tag;
    int priority;
    OperatorType type;
    Node node;

    MatchedOperator(const Operator & op, Node node_) : tag(op.tag), priority(op.priority), type(op.type), node(std::move(node_)) {}
};

using Operators = std::vector<Operator>;

template <typename Term>
struct OperatorExpression : Helper<OperatorExpression<Term>>
{
    Term term;
    Operators operators;

    OperatorExpression(Term term_, Operators operators_) : term(std::move(term_)), operators(std::move(operators_)) {}

    bool match(TokenIterator & it, Node & node) const
    {
        std::stack<MatchedOperator> stack;
        std::vector<Node> nodes;
        bool previous_is_term = false;

        auto bind = [&]
        {
            if (stack.empty())
                return false;

            std::cerr << "Binding\n";

            OperatorType type = stack.top().type;
            std::cerr << "Binding operator ";
            std::cerr.write(stack.top().node.token.begin, stack.top().node.token.size());
            std::cerr << "\n";

            if (type == OperatorType::Infix)
            {
                NodeType current_op = stack.top().tag;
                node = stack.top().node;
                /// Bind a sequence of identical operators like a * b * c.
                while (!stack.empty() && current_op == stack.top().tag)
                {
                    if (nodes.size() < 2)
                        return false;
                    node.children.push_back(nodes.back());
                    nodes.pop_back();
                    node.children.push_back(nodes.back());
                    nodes.pop_back();
                    stack.pop();
                }
                nodes.emplace_back(std::move(node));
                return true;
            }
            else if (type == OperatorType::Prefix)
            {
                if (nodes.empty())
                    return false;
                node = stack.top().node;
                node.children.push_back(nodes.back());
                stack.pop();
                nodes.pop_back();
                nodes.emplace_back(std::move(node));
                return true;
            }
            else if (type == OperatorType::InfixSuffix || type == OperatorType::Ternary)
            {
                if (nodes.size() < 2)
                    return false;
                stack.pop();
                if (stack.empty())
                    return false;
                node = stack.top().node;
                stack.pop();
                node.children.push_back(nodes.back());
                nodes.pop_back();
                nodes.emplace_back(std::move(node));
                return true;
            }

            return false;
        };

        while (true)
        {
            std::cerr << "!" << getNodeName(node.type) << "\n";
            if (!previous_is_term && term.match(it, node))
            {
                nodes.push_back(node);
                previous_is_term = true;
            }
            else
            {
                bool matched = false;
                for (const auto & op : operators)
                {
                    std::cerr << "!!" << getNodeName(op.tag) << "\n";

                    if ((((op.type == OperatorType::Infix || op.type == OperatorType::Ternary || op.type == OperatorType::InfixSuffix)
                            && previous_is_term)
                            || (op.type == OperatorType::Prefix && !previous_is_term))
                        && (op.main.match(it, node) || (op.type == OperatorType::Ternary && op.second.match(it, node))))
                    {
                        Node matched_op_node = node;
                        if (!stack.empty() && op.priority > stack.top().priority)
                            if (!bind())
                                return false;
                        stack.emplace(op, matched_op_node);
                        matched = true;
                        std::cerr << "Matched !\n";
                        break;
                    }
                    else if (op.type == OperatorType::InfixSuffix && op.second.match(it, node))
                    {
                        Node matched_op_node = node;
                        if (!bind())
                            return false;
                        stack.emplace(op, matched_op_node);
                        matched = true;
                        std::cerr << "Matched !!\n";
                        break;
                    }
                }

                if (!matched)
                {
                    bind();
                    break;
                }

                std::cerr << "Matched operator ";
                std::cerr.write(node.token.begin, node.token.size());
                std::cerr << "\n";

                previous_is_term = false;
            }
        }

        return true;
    }
};


}

}
