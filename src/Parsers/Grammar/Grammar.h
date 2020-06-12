#pragma once

#include <iostream>
#include <vector>
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
        default:
            __builtin_unreachable();
    }
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
            if (nested_node.type != NodeType::NoType)
            {
                node.children.emplace_back(std::move(nested_node));
            }
            else if (!nested_node.children.empty())
            {
                for (auto & child : nested_node.children)
                    node.children.emplace_back(std::move(child));
            }
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

    Tag(NodeType type, Grammar grammar) : type(type), grammar(grammar) {}

    bool matchImpl(TokenIterator & it, Node & node) const
    {
        std::cerr << "Parsing " << getNodeName(type) << std::string(it->begin, it->end) << "\n";
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
    Keyword(const char * s) : s(s), size(strlen(s)) {}

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
    Sequence(Grammars... grammars) : grammars(grammars...) {}

    bool matchImpl(TokenIterator & it, Node & node) const
    {
        return std::apply([&](auto... grammar){ return ( grammar.match(it, node) && ... ); }, grammars);
    }
};

template <typename... Grammars>
struct Alternative : Helper<Alternative<Grammars...>>
{
    std::tuple<Grammars...> grammars;
    Alternative(Grammars... grammars) : grammars(grammars...) {}

    bool matchImpl(TokenIterator & it, Node & node) const
    {
        return std::apply([&](auto... grammar){ return ( grammar.match(it, node) || ... ); }, grammars);
    }
};

template <typename Grammar>
struct Optional : Helper<Optional<Grammar>>
{
    Grammar grammar;
    Optional(Grammar grammar) : grammar(grammar) {}

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
    ZeroOrMore(Grammar grammar) : grammar(grammar) {}

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
    OneOrMore(Grammar grammar) : grammar(grammar) {}

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
    DelimitedSequence(Element element, Delimiter delimiter) : element(element), delimiter(delimiter) {}

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
    ProperDelimitedSequence(Element element, Delimiter delimiter) : element(element), delimiter(delimiter) {}

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

    template <typename Grammar>
    Indirect & operator=(Grammar grammar)
    {
        *impl = [grammar](TokenIterator & it, Node & node) { return grammar.match(it, node); };
        return *this;
    }

    bool match(TokenIterator & it, Node & node) const
    {
        return (*impl)(it, node);
    }
};


}

}
